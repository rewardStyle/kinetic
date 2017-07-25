package producer

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/golang/time/rate"
	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
)

// producerOptions holds all of the configurable settings for a Producer
type producerOptions struct {
	batchSize        int                 // maximum message capacity per request
	batchTimeout     time.Duration       // maximum time duration to wait for incoming messages
	queueDepth       int                 // maximum number of messages to enqueue in the message queue
	maxRetryAttempts int                 // maximum number of retry attempts for failed messages
	concurrency      int                 // number of concurrent workers per shard
	shardCheckFreq   time.Duration       // frequency (specified as a duration) with which to check the the shard size
	dataSpillFn      MessageHandlerAsync // callback function for handling dropped messages that the producer was unable to send to the stream
	Stats            StatsCollector      // stats collection mechanism
}

// Producer sends records to AWS Kinesis or Firehose.
type Producer struct {
	*producerOptions                         // contains all of the configuration settings for the Producer
	*logging.LogHelper                       // object for help with logging
	writer             StreamWriter          // interface for abstracting the PutRecords call
	msgCountLimiter    *rate.Limiter         // rate limiter to limit the number of messages dispatched per second
	msgSizeLimiter     *rate.Limiter         // rate limiter to limit the total size (in bytes) of messages dispatched per second
	workerCount        int                   // number of concurrent workers sending batch messages for the producer
	messages           chan *message.Message // channel for enqueuing messages to be put on the stream
	status             chan *statusReport    // channel for workers to communicate their current status
	dismiss            chan empty            // channel for handling the decommissioning of a surplus of workers
	stop               chan empty            // channel for handling shutdown
	pipeOfDeath        chan empty            // channel for handling pipe of death
	startupOnce        sync.Once             // used to ensure that the startup function is called once
	shutdownOnce       sync.Once             // used to ensure that the shutdown function is called once
	resizeMu           sync.Mutex            // used to prevent resizeWorkerPool from being called synchronously with itself
	shardCountMu       sync.Mutex            // used to add thread-safe access to the shardCount variable
	noCopy             noCopy                // prevents the Producer from being copied
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
			Logger:   cfg.AwsConfig.Logger,
		},
		writer: w,
	}, nil
}

// produce is called once to initialize a pool of workers which send batches of messages concurrently
func (p *Producer) produce() {
	p.startupOnce.Do(func() {
		// Reset shutdownOnce to allow the shut down sequence to happen again
		p.shutdownOnce = sync.Once{}

		// Instantiate rate limiters
		p.msgCountLimiter = rate.NewLimiter(rate.Limit(float64(p.writer.getMsgCountRateLimit())), p.batchSize)
		p.msgSizeLimiter = rate.NewLimiter(rate.Limit(float64(p.writer.getMsgSizeRateLimit())), p.writer.getMsgSizeRateLimit())

		// Create communication channels
		p.messages = make(chan *message.Message, p.queueDepth)
		p.status = make(chan *statusReport)
		p.dismiss = make(chan empty)
		p.stop = make(chan empty)
		p.pipeOfDeath = make(chan empty)

		// Run a separate go routine to check the shard size (throughput multiplier) and resize the worker pool
		// periodically if needed
		stopShardCheck := make(chan empty)
		go func() {
			var mult int

			timer := time.NewTicker(p.shardCheckFreq)
			for {
				newMult, err := p.writer.getConcurrencyMultiplier()
				if err != nil {
					p.LogError("Failed to call getConcurrencyMultiplier due to: ", err)
				}
				if newMult != mult && newMult > 0 {
					p.resizeWorkerPool(newMult * p.concurrency)
					p.Stats.UpdateProducerConcurrency(newMult * p.concurrency)
					mult = newMult
				}

				select {
				case <-stopShardCheck:
					timer.Stop()
					return
				case <-timer.C:
					break
				}
			}
		}()

		// Dispatch messages to each worker depending on the worker's capacity and the rate limit
		go func() {
			defer func() {
				// Stop the periodic shard size check
				stopShardCheck <- empty{}
				close(stopShardCheck)
			}()

			for {
				select {
				case <-p.pipeOfDeath:
					return
				case <-p.stop:
					return
				case status := <-p.status:
					var batch []*message.Message

					// Start pulling from the message channel and waiting for the bucket token to
					// fill at the same time
					wg := sync.WaitGroup{}
					wg.Add(1)
					go func() {
						defer wg.Done()

						var batchMsgSize int
						timeout := time.After(p.batchTimeout)

					fillBatch:
						for len(batch) < status.capacity {
							select {
							case <-timeout:
								break fillBatch
							case msg := <-p.messages:
								if msg == nil {
									// Drop nil message
									break
								}

								var msgSize int
								if msgSize = msg.RequestEntrySize(); msgSize > p.writer.getMsgSizeRateLimit() {
									p.sendToDataSpill(msg)
									break
								}
								batchMsgSize += msgSize
								batch = append(batch, msg)
							}
						}

						// Request and wait for the message size (transmission) rate limiter to
						// allow this payload
						ctx, cancel := context.WithTimeout(context.TODO(), p.batchTimeout)
						defer cancel()
						if err := p.msgSizeLimiter.WaitN(ctx, batchMsgSize+status.failedSize); err != nil {

						}
						cancel()
					}()

					wg.Add(1)
					go func() {
						defer wg.Done()

						// Request and wait for the message counter rate limiter to allow this
						// payload
						ctx, cancel := context.WithTimeout(context.TODO(), p.batchTimeout)
						err := p.msgCountLimiter.WaitN(ctx, status.capacity+status.failedCount)
						if err != nil {
							// TODO: handle error properly
						}
						cancel()
					}()

					// Wait until the batch is ready and the rate limiter gives the go ahead
					wg.Wait()

					// Send batch regardless if it is empty or not
					status.channel <- batch
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
					for msg := range p.messages {
						p.sendToDataSpill(msg)
					}
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
		failedCount := len(retries)
		if !dismissed {
			capacity = p.batchSize - failedCount
		}
		var failedSize int
		for _, msg := range retries {
			failedSize += msg.RequestEntrySize()
		}
		status := &statusReport{
			capacity:    capacity,
			failedCount: failedCount,
			failedSize:  failedSize,
			channel:     batches,
		}
		p.status <- status

		// Receive a batch of messages and call the producer's sendBatch function
		batch := <-batches
		if len(batch)+len(retries) > 0 {
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
			p.sendToDataSpill(msg)
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

// sendToDataSpill is called when the producer is unable to write the message to the stream
func (p *Producer) sendToDataSpill(msg *message.Message) {
	p.Stats.AddDroppedTotal(1)
	p.Stats.AddDroppedCapacity(1)
	if err := p.dataSpillFn(msg); err != nil {
		p.LogError("Unable to call data spill function on message: ", string(msg.Data))
	}
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
		p.sendToDataSpill(msg)
		return errs.ErrDroppedMessage
	}
}
