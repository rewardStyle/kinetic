package producer

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/errs"
)

// producerOptions holds all of the configurable settings for a Producer
type producerOptions struct {
	batchSize        int            // maximum message capacity per request
	batchTimeout     time.Duration  // maximum time duration to wait for incoming messages
	queueDepth       int            // maximum number of messages to enqueue in the message queue
	maxRetryAttempts int            // maximum number of retry attempts for failed messages
	workersPerShard  int            // number of concurrent workers per shard
	shardCount       int            // initial shard size
	rateLimit        int            // maximum records to be sent per cycle for the rate limiting model
	resetFrequency   time.Duration  // duration of a cycle for the rate limiting model
	Stats            StatsCollector // stats collection mechanism
}

// Producer sends records to AWS Kinesis or Firehose.
type Producer struct {
	*producerOptions                            // contains all of the configuration settings for the Producer
	*logging.LogHelper                          // object for help with logging
	writer         StreamWriter                 // interface for abstracting the PutRecords call
	rateLimiter    *rateLimiter                 // throttles the number of messages sent based on total count and size
	workerCount    int                          // number of concurrent workers sending batch messages for the producer
	messages       chan *message.Message        // channel for enqueuing messages to be put on the stream
	status         chan *statusReport           // channel for workers to communicate their current status
	dismiss        chan empty                   // channel for handling the decommissioning of a surplus of workers
	stop           chan empty                   // channel for handling shutdown
	pipeOfDeath    chan empty                   // channel for handling pipe of death
	startupOnce    sync.Once                    // used to ensure that the startup function is called once
	shutdownOnce   sync.Once                    // used to ensure that the shutdown function is called once
	resizeMu       sync.Mutex                   // used to prevent resizeWorkerPool from being called synchronously with itself
	noCopy         noCopy                       // prevents the Producer from being copied
}

// NewProducer creates a new producer for writing records to a Kinesis or Firehose stream.
func NewProducer(c *aws.Config, w StreamWriter, fn ...func(*Config)) (*Producer, error) {
	cfg := NewConfig(c)
	for _, f := range fn {
		f(cfg)
	}
	return &Producer{
		producerOptions: cfg.producerOptions,
		LogHelper: &logging.LogHelper{
			LogLevel: cfg.LogLevel,
			Logger: cfg.AwsConfig.Logger,
		},
		writer: w,
	}, nil
}

// produce is called once to initialize a pool of workers which send batches of messages concurrently
func (p *Producer) produce() {
	p.startupOnce.Do(func() {
		// Reset shutdownOnce to allow the shut down sequence to happen again
		p.shutdownOnce = sync.Once{}

		// Create communication channels
		p.rateLimiter = newRateLimiter(p.rateLimit, p.resetFrequency)
		p.messages = make(chan *message.Message, p.queueDepth)
		p.status = make(chan *statusReport)
		p.dismiss = make(chan empty)
		p.stop = make(chan empty)
		p.pipeOfDeath = make(chan empty)

		// Instantiate and register new workers
		p.resizeWorkerPool(p.shardCount * p.workersPerShard)

		// Instantiate and start a new rate limiter
		p.rateLimiter.start()

		go func(){
			// Dispatch messages to each worker depending on the worker's capacity and the rate limit
			for {
				select {
				case <-p.pipeOfDeath:
					return
				case <-p.stop:
					return
				case status := <-p.status:
					var tokenCount int
					for tokenCount = p.rateLimiter.getTokenCount(); tokenCount == 0; {
						// Wait for a reset notification from the rateLimiter if needed
						<-p.rateLimiter.resetChannel
						tokenCount = p.rateLimiter.getTokenCount()
					}

					var batch []*message.Message
					timeout := time.After(p.batchTimeout)

					fillBatch:
					for len(batch) < status.capacity && len(batch) + status.failed < tokenCount {
						select {
						case <-timeout:
							break fillBatch
						case msg := <-p.messages:
							if msg != nil {
								batch = append(batch, msg)
							}
						}
					}

					// Claim tokens only if needed
					if len(batch) + status.failed > 0 {
						p.rateLimiter.claimTokens(len(batch) + status.failed)
					}

					// Send batch regardless if it is empty or not
					status.channel <-batch
				}
			}
		}()
	})
}

// shutdown is called once to handle the graceful shutdown of the produce function
func (p *Producer) shutdown() {
	p.shutdownOnce.Do(func() {
		// Close the messages channel to prevent any more incoming messages
		if p.messages != nil {
			close(p.messages)
		}

		// Allow the workers to drain the message channel first
		staleTimeout := time.Duration(3 * time.Second)
		timer := time.NewTimer(staleTimeout)
		remaining := len(p.messages)

		drain:
		for remaining > 0 {
			select {
			case <-time.After(time.Second):
				newRemaining := len(p.messages)
				if newRemaining != remaining {
					timer.Reset(staleTimeout)
					remaining = newRemaining
				}
			case <-timer.C:
				timer.Stop()
				if remaining > 0 {
					// TODO:  Send remaining messages to the data spill
				}
				break drain
			}
		}

		// Decommission all the workers
		p.resizeWorkerPool(0)

		// Close the decommission channel
		if p.dismiss != nil {
			close(p.dismiss)
		}

		// Close the status channel
		if p.status != nil {
			close(p.status)
		}

		// Stop the rate limiter
		p.rateLimiter.stop()

		// Stop the running go routine in produce
		p.stop <- empty{}

		// Reset startupOnce to allow the start up sequence to happen again
		p.startupOnce = sync.Once{}
	})
}

// resizeWorkerPool is called to instantiate new workers, decommission workers or recommission workers that have been
// deactivated
func (p *Producer) resizeWorkerPool(desiredWorkerCount int) {
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()

	if p.workerCount < desiredWorkerCount {
		for p.workerCount < desiredWorkerCount {
			go p.newWorker()
			p.workerCount++
		}
	} else {
		for p.workerCount > desiredWorkerCount {
			p.dismiss <- empty{}
			p.workerCount--
		}
	}
}

// newWorker is a (blocking) helper function that increases the number of concurrent go routines calling the
// sendBatch function.  Communications between the produce function and the newWorker function occurs on the status
// channel where the "worker" provides information regarding its previously failed message count, its capacity for
// new messages and the "worker's" channel to which the produce function should send the batches.  The "workers" also
// listen on the dismiss channel which upon receiving a signal will continue sending previously failed messages only
// until all failed messages have been sent successfully or aged out.
func (p *Producer) newWorker() {
	batches := make(chan []*message.Message)
	defer close(batches)

	var retries []*message.Message
	var dismissed bool
	for ok := true; ok; ok = !dismissed || len(retries) != 0 {
		// Check to see if there were any signals to dismiss workers (if eligible)
		if !dismissed {
			select {
			case <-p.dismiss:
				dismissed = true
			default:
			}
		}

		// Send a status report to the status channel based on the number of previously failed messages and
		// whether the worker has been dismissed
		var capacity int
		if !dismissed {
			capacity = p.batchSize - len(retries)
		}
		status := &statusReport {
			capacity: capacity,
			failed: len(retries),
			channel: batches,
		}
		p.status <-status

		// Receive a batch of messages and call the producer's sendBatch function
		batch := <-batches
		if len(batch) + len(retries) > 0 {
			retries = p.sendBatch(append(retries, batch...))
		}

	}
}

// sendBatch is the function that is called by each worker to put records on the stream. sendBatch accepts a slice of
// messages to send and returns a slice of messages that failed to send
func (p *Producer) sendBatch(batch []*message.Message) []*message.Message {
	var failed []*message.Message
	err := p.writer.PutRecords(context.TODO(), batch, func(msg *message.Message) error {
		if msg.FailCount <= p.maxRetryAttempts {
			failed = append(failed, msg)
			p.Stats.AddSentRetried(1)
		} else {
			p.Stats.AddDroppedTotal(1)
			p.Stats.AddDroppedRetries(1)
		}
		return nil
	})
	if err == nil {
		p.Stats.AddSentTotal(len(batch))
		return failed
	}

	// Beyond this point the PutRecords API call failed for some reason
	switch err := err.(type) {
	case net.Error:
		if err.Timeout() {
			p.Stats.AddPutRecordsTimeout(1)
			p.LogError("Received net error:", err.Error())
		} else {
			p.LogError("Received unknown net error:", err.Error())
		}
	case awserr.Error:
		p.LogError("Received AWS error:", err.Error())
	case error:
		switch err {
		case errs.ErrRetryRecords:
			break
		default:
			p.LogError("Received error:", err.Error())
		}
	default:
		p.LogError("Received unknown error:", err.Error())
	}

	return batch
}

// CloseWithContext initiates the graceful shutdown of the produce function, waiting for all outstanding messages and to
// flush.  Cancellation is supported through contexts.
func (p *Producer) CloseWithContext(ctx context.Context) {
	p.shutdown()
	<-ctx.Done()
	close(p.pipeOfDeath)
}

// Close initiates the graceful shutdown of the produce function, waiting for all outstanding messages and to flush.
func (p *Producer) Close() {
	p.CloseWithContext(context.TODO())
}

// SendWithContext sends a message to the stream.  Cancellation supported through contexts.
func (p *Producer) SendWithContext(ctx context.Context, msg *message.Message) error {
	p.produce()
	select {
	case p.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Send a message to the stream, waiting on the message to be put into the channel.
func (p *Producer) Send(msg *message.Message) error {
	return p.SendWithContext(context.TODO(), msg)
}

// TryToSend will attempt to send a message to the stream if the channel has capacity for a message, or will immediately
// return with an error if the channel is full.
func (p *Producer) TryToSend(msg *message.Message) error {
	p.produce()
	select {
	case p.messages <- msg:
		return nil
	default:
		p.Stats.AddDroppedTotal(1)
		p.Stats.AddDroppedCapacity(1)
		return errs.ErrDroppedMessage
	}
}
