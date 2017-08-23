package kinetic

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"golang.org/x/time/rate"
)

// consumerOptions is used to hold all of the configurable settings of a Consumer.
type consumerOptions struct {
	reader      StreamReader           // interface for abstracting the GetRecord/GetRecords call
	queueDepth  int                    // size of the consumer's message channel
	concurrency int                    // number of concurrent routines processing messages off of the message channel
	logLevel    aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats       ConsumerStatsCollector // stats collection mechanism
}

// defaultConsumerOptions instantiates a consumerOptions with default values.
func defaultConsumerOptions() *consumerOptions {
	return &consumerOptions{
		queueDepth:  10000,
		concurrency: 10,
		Stats:       &NilConsumerStatsCollector{},
	}
}

// ConsumerOptionsFn is a method signature for defining functional option methods for configuring the Consumer.
type ConsumerOptionsFn func(*Consumer) error

// ConsumerReader is a functional option method for configuring the consumer's stream reader.
func ConsumerReader(r StreamReader) ConsumerOptionsFn {
	return func(o *Consumer) error {
		o.reader = r
		return nil
	}
}

// ConsumerQueueDepth is a functional option method for configuring the consumer's queueDepth.
func ConsumerQueueDepth(depth int) ConsumerOptionsFn {
	return func(o *Consumer) error {
		if depth > 0 {
			o.queueDepth = depth
			return nil
		}
		return ErrInvalidQueueDepth
	}
}

// ConsumerConcurrency is a functional option method for configuring the consumer's concurrency.
func ConsumerConcurrency(count int) ConsumerOptionsFn {
	return func(o *Consumer) error {
		if count > 0 {
			o.concurrency = count
			return nil
		}
		return ErrInvalidConcurrency
	}
}

// ConsumerLogLevel is a functional option method for configuring the consumer's log level.
func ConsumerLogLevel(ll aws.LogLevelType) ConsumerOptionsFn {
	return func(o *Consumer) error {
		o.logLevel = ll & 0xffff0000
		return nil
	}
}

// ConsumerStats is a functional option method for configuring the consumer's stats collector.
func ConsumerStats(sc ConsumerStatsCollector) ConsumerOptionsFn {
	return func(o *Consumer) error {
		o.Stats = sc
		return nil
	}
}

// Consumer polls the StreamReader for messages.
type Consumer struct {
	*consumerOptions                  // contains all of the configuration settings for the Consumer
	*LogHelper                        // object for help with logging
	txnCountRateLimiter *rate.Limiter // rate limiter to limit the number of transactions per second
	txSizeRateLimiter   *rate.Limiter // rate limiter to limit the transmission size per seccond
	messages            chan *Message // channel for storing messages that have been retrieved from the stream
	concurrencySem      chan empty    // channel for controlling the number of concurrent workers processing messages from the message channel
	pipeOfDeath         chan empty    // channel for handling pipe of death
	consuming           bool          // flag for indicating whether or not the consumer is consuming
	consumingMu         sync.Mutex    // mutex for making the consuming flag thread safe
	noCopy              noCopy        // prevents the Consumer from being copied
}

// NewConsumer creates a new Consumer object for retrieving and listening to message(s) on a StreamReader.
func NewConsumer(c *aws.Config, stream string, shard string, optionFns ...ConsumerOptionsFn) (*Consumer, error) {
	consumer := &Consumer{consumerOptions: defaultConsumerOptions()}
	for _, optionFn := range optionFns {
		optionFn(consumer)
	}

	if consumer.reader == nil {
		r, err := NewKinesisReader(c, stream, shard)
		if err != nil {
			return nil, err
		}
		consumer.reader = r
	}

	consumer.LogHelper = &LogHelper{
		LogLevel: consumer.logLevel,
		Logger:   c.Logger,
	}

	return consumer, nil
}

// startConsuming will initialize the message channel and set consuming to true if there is not already another consume
// loop running.
func (c *Consumer) startConsuming() bool {
	c.consumingMu.Lock()
	defer c.consumingMu.Unlock()
	if !c.consuming {
		c.consuming = true
		c.messages = make(chan *Message, c.queueDepth)
		c.concurrencySem = make(chan empty, c.concurrency)
		c.pipeOfDeath = make(chan empty)
		return true
	}
	return false
}

// shouldConsume is a convenience function that allows functions to break their loops if the context receives a
// cancellation or a pipe of death.
func (c *Consumer) shouldConsume(ctx context.Context) (bool, error) {
	select {
	case <-c.pipeOfDeath:
		return false, ErrPipeOfDeath
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		return true, nil
	}
}

// stopConsuming handles any cleanup after consuming has stopped.
func (c *Consumer) stopConsuming() {
	c.consumingMu.Lock()
	defer c.consumingMu.Unlock()
	if c.consuming && c.messages != nil {
		close(c.messages)
	}
	c.consuming = false
}

// enqueueSingle calls the readers's GetRecord method and enqueus a single message on the message channel.
func (c *Consumer) enqueueSingle(ctx context.Context) (count int, size int) {
	var err error
	count, size, err = c.reader.GetRecord(ctx,
		func(msg *Message) error {
			c.messages <- msg
			return nil
		})
	if err != nil {
		c.handleErrorLogging(err)
	}

	return count, size
}

// enqueueBatch calls the reader's GetRecords method and enqueues a batch of messages on the message chanel.
func (c *Consumer) enqueueBatch(ctx context.Context) (count, size int) {
	var err error
	count, size, err = c.reader.GetRecords(ctx,
		func(msg *Message) error {
			c.messages <- msg
			return nil
		})
	if err != nil {
		c.handleErrorLogging(err)
	}

	return count, size
}

// handleErrorLogging is a helper method for handling and logging errors from calling the reader's
// GetRecord and GetRecords method.
func (c *Consumer) handleErrorLogging(err error) {
	switch err := err.(type) {
	case net.Error:
		if err.Timeout() {
			c.Stats.AddGetRecordsTimeout(1)
			c.LogError("Received net error:", err.Error())
		} else {
			c.LogError("Received unknown net error:", err.Error())
		}
	case error:
		switch err {
		case ErrTimeoutReadResponseBody:
			c.Stats.AddGetRecordsReadTimeout(1)
			c.LogError("Received error:", err.Error())
		default:
			c.LogError("Received error:", err.Error())
		}
	default:
		c.LogError("Received unknown error:", err.Error())
	}
}

// RetrieveWithContext waits for a message from the stream and returns the  Cancellation is supported through
// contexts.
func (c *Consumer) RetrieveWithContext(ctx context.Context) (*Message, error) {
	if !c.startConsuming() {
		return nil, ErrAlreadyConsuming
	}
	defer c.stopConsuming()

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		// A cancellation or closing the pipe of death will cause Retrieve (and related functions) to abort in
		// between getRecord calls.  Note, that this would only occur when there are no new records to retrieve.
		// Otherwise, getRecords will be allowed to run to completion and deliver one record.
		ok, err := c.shouldConsume(ctx)
		if !ok {
			return nil, err
		}
		n, _ := c.enqueueSingle(childCtx)
		if n > 0 {
			c.Stats.AddDelivered(n)
			return <-c.messages, nil
		}
	}
}

// Retrieve waits for a message from the stream and returns the value.
func (c *Consumer) Retrieve() (*Message, error) {
	return c.RetrieveWithContext(context.TODO())
}

// RetrieveFnWithContext retrieves a message from the stream and dispatches it to the supplied function.  RetrieveFn
// will wait until the function completes. Cancellation is supported through context.
func (c *Consumer) RetrieveFnWithContext(ctx context.Context, fn MessageProcessor) error {
	msg, err := c.RetrieveWithContext(ctx)
	if err != nil {
		return err
	}

	start := time.Now()
	fn(msg)
	c.Stats.UpdateProcessedDuration(time.Since(start))
	c.Stats.AddProcessed(1)
	return nil
}

// RetrieveFn retrieves a message from the stream and dispatches it to the supplied function.  RetrieveFn will wait
// until the function completes.
func (c *Consumer) RetrieveFn(fn MessageProcessor) error {
	return c.RetrieveFnWithContext(context.TODO(), fn)
}

// consume calls getRecords with configured batch size in a loop until the consumer is stopped.
func (c *Consumer) consume(ctx context.Context) {
	// We need to run startConsuming to make sure that we are okay and ready to start consuming.  This is mainly to
	// avoid a race condition where Listen() will attempt to read the messages channel prior to consume()
	// initializing it.  We can then launch a goroutine to handle the actual consume operation.
	if !c.startConsuming() {
		return
	}
	go func() {
		defer c.stopConsuming()

		// TODO: make these parameters configurable also scale according to the shard count
		c.txnCountRateLimiter = rate.NewLimiter(rate.Limit(5), 1)
		c.txSizeRateLimiter = rate.NewLimiter(rate.Limit(2000000), 2000000)

		childCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			// The consume loop can be cancelled by a calling the cancellation function on the context or by
			// closing the pipe of death.  Note that in the case of context cancellation, the getRecords
			// call below will be allowed to complete (as getRecords does not regard context cancellation).
			// In the case of cancellation by pipe of death, however, the getRecords will immediately abort
			// and allow the consume function to immediately abort as well.
			if ok, _ := c.shouldConsume(ctx); !ok {
				return
			}

			_, size := c.enqueueBatch(childCtx)

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()

				if err := c.txnCountRateLimiter.Wait(childCtx); err != nil {
					c.LogError("Error occured waiting for transaction count tokens")
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := c.txSizeRateLimiter.WaitN(childCtx, size); err != nil {
					c.LogError("Error occured waiting for transmission size tokens")
				}
			}()
			wg.Wait()
		}
	}()
}

// ListenWithContext listens and delivers message to the supplied function.  Upon cancellation, Listen will stop the
// consumer loop and wait until the messages channel is closed and all messages are delivered.
func (c *Consumer) ListenWithContext(ctx context.Context, fn MessageProcessor) {
	c.consume(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case msg, ok := <-c.messages:
			if !ok {
				return
			}
			c.Stats.AddDelivered(1)
			// For simplicity, did not do the pipe of death here. If POD is received, we may deliver a
			// couple more messages (especially since select is random in which channel is read from).
			c.concurrencySem <- empty{}
			wg.Add(1)
			go func(msg *Message) {
				defer func() {
					<-c.concurrencySem
				}()
				start := time.Now()
				fn(msg)
				c.Stats.UpdateProcessedDuration(time.Since(start))
				c.Stats.AddProcessed(1)
				wg.Done()
			}(msg)
		case <-c.pipeOfDeath:
			c.LogInfo("ListenWithContext received pipe of death")
			return
		}
	}
}

// Listen listens and delivers message to the supplied function.
func (c *Consumer) Listen(fn MessageProcessor) {
	c.ListenWithContext(context.TODO(), fn)
}
