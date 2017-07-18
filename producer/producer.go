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
	*producerOptions			// contains all of the configuration settings for the Producer
	*logging.LogHelper			// object for help with logging
	writer         StreamWriter		// interface for abstracting the PutRecords call
	rateLimiter    *rateLimiter		// throttles the number of messages sent based on total count and size
	workerRegistry map[string]*worker	// roster of workers in the worker pool
	messages       chan *message.Message	// channel for enqueuing messages to be put on the stream
	statusChannel  chan *statusReport	// channel for workers to communicate their current status
	decommChannel  chan empty		// channel for handling the decommissioning of a surplus of workers
	stopChannel    chan empty		// channel for handling shutdown
	pipeOfDeath    chan empty		// channel for handling pipe of death
	startupOnce    sync.Once		// used to ensure that the startup function is called once
	shutdownOnce   sync.Once		// used to ensure that the shutdown function is called once
	resizeMu       sync.Mutex		// used to prevent resizeWorkerPool from being called synchronously with itself
	noCopy         noCopy			// prevents the Producer from being copied
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
		workerRegistry: make(map[string]*worker),
		rateLimiter: newRateLimiter(cfg.rateLimit, cfg.resetFrequency),
		messages: make(chan *message.Message, cfg.queueDepth),
		statusChannel: make(chan *statusReport),
		decommChannel: make(chan empty),
		stopChannel: make(chan empty),
		pipeOfDeath: make(chan empty),
	}, nil
}

// produce is called once to initialize a pool of workers which send batches of messages concurrently
func (p *Producer) produce() {
	p.startupOnce.Do(func() {
		// Reset shutdownOnce to allow the shut down sequence to happen again
		p.shutdownOnce = sync.Once{}

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
				case <-p.stopChannel:
					return
				case req := <-p.statusChannel:
					// Use the worker registry to find the worker by workerID
					worker, ok := p.workerRegistry[req.workerID];
					if !ok {
						// move on to the next status if a bogus workerID is provided
						break
					}

					// Verify that worker is in the right state for handling a new batch of messages
					workerState := worker.getWorkerState()
					if !(workerState == workerStateIdle ||
						workerState == workerStateIdleWithRetries) {
						// otherwise on to the next one
						break
					}

					// If we need to decommission some workers distribute the decommission command
				        // to idle workers, otherwise try to fill the workers capacity
					var batch []*message.Message
					var decommissioned bool
					if len(p.decommChannel) > 0 && workerState == workerStateIdle {
						<-p.decommChannel
						decommissioned = true
					} else {
						tokenCount := p.rateLimiter.getTokenCount()
						timeout := time.After(req.timeout)

						fillBatch:
						for len(batch) < req.capacity {
							select {
							case <-timeout:
								break fillBatch
							case msg := <-p.messages:
								if msg != nil {
									batch = append(batch, msg)
								}
							}
							if len(batch) + req.failed >= tokenCount {
								break fillBatch
							}
						}
						p.rateLimiter.claimTokens(len(batch) + req.failed)
					}

					// Send the command to the worker's command channel
					worker.commands <-&workerCommand{
						batchMsgs: batch,
						decommissioned: decommissioned,
					}
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
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func(){
			defer wg.Done()

			staleTimeout := time.Duration(3 * time.Second)
			timer := time.NewTimer(staleTimeout)
			remaining := len(p.messages)
			for remaining > 0 {
				select {
				case <-time.After(time.Second):
					newRemaining := len(p.messages)
					if newRemaining != remaining {
						timer.Reset(staleTimeout)
						remaining = newRemaining
					}
				case <-timer.C:
					return
				}
			}
		}()
		wg.Wait()

		// Decommission all the workers
		p.resizeWorkerPool(0)

		// Close the decommission channel
		if p.decommChannel != nil {
			close(p.decommChannel)
		}

		// Close the status channel
		if p.statusChannel != nil {
			close(p.statusChannel)
		}

		// Stop the rate limiter
		p.rateLimiter.stop()

		// Stop the running go routine in produce
		p.stopChannel <- empty{}

		// Reset startupOnce to allow the start up sequence to happen again
		p.startupOnce = sync.Once{}
	})
}

// resizeWorkerPool is called to instantiate new workers, decommission workers or recommission workers that have been
// deactivated
func (p *Producer) resizeWorkerPool(desiredWorkerCount int) {
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()

	// Get the number of total workers in the registry
	totalWorkerCount := len(p.workerRegistry)


	// Create a map of available workers
	availableWorkers := make(map[string]*worker, totalWorkerCount)
	for id, worker := range p.workerRegistry {
		workerState := worker.getWorkerState()
		if workerState == workerStateInactive || workerState == workerStateDecommissioned {
			availableWorkers[id] = worker
		}
	}
	activeWorkerCount := totalWorkerCount - len(availableWorkers)

	// We have too many workers at present than we actually need
	if desiredWorkerCount < activeWorkerCount {
		// Decommission the workers that we don't need
		for i := activeWorkerCount; i <= desiredWorkerCount; i-- {
			p.decommChannel <-empty{}
		}
	// We need more workers than are presently active or commissioned
	} else if activeWorkerCount < desiredWorkerCount {
		// Recommission those workers that are inactive
		var activated int
		for _, worker := range availableWorkers {
			if activated > desiredWorkerCount - activeWorkerCount {
				break
			}
			worker.start()
			activated++
		}

		// Spawn new workers if still not enough
		if desiredWorkerCount > totalWorkerCount {
			for i := totalWorkerCount; i < desiredWorkerCount; i++ {
				worker := newWorker(p.producerOptions, p.sendBatch, p.reportStatus)
				p.workerRegistry[worker.workerID] = worker
			}
		}
	}
}

// sendBatch is the function that is called by each worker to put records on the stream. sendBatch accepts a slice of
// messages to send and returns a slice of messages that failed to send
func (p *Producer) sendBatch(batch []*message.Message) []*message.Message {
	var failed []*message.Message
	err := p.writer.PutRecords(context.TODO(), batch, func(msg *message.Message, wg *sync.WaitGroup) error {
		defer wg.Done()

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

// reportStatus is used as a closure for the workers to report status to
func (p *Producer) reportStatus(report *statusReport) {
	p.statusChannel <- report
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
