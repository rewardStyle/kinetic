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
	queueDepth  int          	   // size of the consumer's message channel
	concurrency int		           // number of concurrent routines processing messages off of the message channel
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
type ConsumerOptionsFn func(*consumerOptions) error

// ConsumerQueueDepth is a functional option method for configuring the consumer's queueDepth.
func ConsumerQueueDepth(depth int) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		if depth > 0 {
			o.queueDepth = depth
			return nil
		}
		return ErrInvalidQueueDepth
	}
}

// ConsumerConcurrency is a functional option method for configuring the consumer's concurrency.
func ConsumerConcurrency(count int) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		if count > 0 {
			o.concurrency = count
			return nil
		}
		return ErrInvalidConcurrency
	}
}

// ConsumerLogLevel is a functional option method for configuring the consumer's log level.
func ConsumerLogLevel(ll aws.LogLevelType) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		o.logLevel = ll & 0xffff0000
		return nil
	}
}

// ConsumerStats is a functional option method for configuring the consumer's stats collector.
func ConsumerStats(sc ConsumerStatsCollector) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		o.Stats = sc
		return nil
	}
}

// Consumer polls the StreamReader for messages.
type Consumer struct {
	*consumerOptions
	*LogHelper
	reader              StreamReader
	txnCountRateLimiter *rate.Limiter
	txSizeRateLimiter   *rate.Limiter
	messages            chan *Message
	concurrencySem      chan empty
	pipeOfDeath         chan empty
	consuming           bool
	consumingMu         sync.Mutex
}

// NewConsumer creates a new Consumer object for retrieving and listening to message(s) on a StreamReader.
func NewConsumer(c *aws.Config, r StreamReader, optionFns ...ConsumerOptionsFn) (*Consumer, error) {
	consumerOptions := defaultConsumerOptions()
	for _, optionFn := range optionFns {
		optionFn(consumerOptions)
	}
	return &Consumer{
		consumerOptions: consumerOptions,
		LogHelper: &LogHelper{
			LogLevel: consumerOptions.logLevel,
			Logger:   c.Logger,
		},
		reader: r,
	}, nil
}

// startConsuming will initialize the message channel and set consuming to true if there is not already another consume
// loop running.
func (l *Consumer) startConsuming() bool {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	if !l.consuming {
		l.consuming = true
		l.messages = make(chan *Message, l.queueDepth)
		l.concurrencySem = make(chan empty, l.concurrency)
		l.pipeOfDeath = make(chan empty)
		return true
	}
	return false
}

// shouldConsume is a convenience function that allows functions to break their loops if the context receives a
// cancellation or a pipe of death.
func (l *Consumer) shouldConsume(ctx context.Context) (bool, error) {
	select {
	case <-l.pipeOfDeath:
		return false, ErrPipeOfDeath
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		return true, nil
	}
}

// stopConsuming handles any cleanup after consuming has stopped.
func (l *Consumer) stopConsuming() {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	if l.consuming && l.messages != nil {
		close(l.messages)
	}
	if l.concurrencySem != nil {
		close(l.concurrencySem)
	}
	l.consuming = false
}

// enqueueSingle calls the readers's GetRecord method and enqueus a single message on the message channel.
func (l *Consumer) enqueueSingle(ctx context.Context) (int, int, error) {
	n, m, err := l.reader.GetRecord(ctx, func(msg *Message, wg *sync.WaitGroup) error {
		defer wg.Done()
		l.messages <- msg

		return nil
	})
	if err != nil {
		l.handleErrorLogging(err)
		return 0, 0, err
	}
	return n, m, nil
}

// enqueueBatch calls the reader's GetRecords method and enqueues a batch of messages on the message chanel.
func (l *Consumer) enqueueBatch(ctx context.Context) (int, int, error) {
	n, m, err := l.reader.GetRecords(ctx,
		func(msg *Message, wg *sync.WaitGroup) error {
			defer wg.Done()
			l.messages <- msg

			return nil
		})
	if err != nil {
		l.handleErrorLogging(err)
		return 0, 0, err
	}
	return n, m, nil
}

// handleErrorLogging is a helper method for handling and logging errors from calling the reader's
// GetRecord and GetRecords method.
func (l *Consumer) handleErrorLogging(err error) {
	switch err := err.(type) {
	case net.Error:
		if err.Timeout() {
			l.Stats.AddGetRecordsTimeout(1)
			l.LogError("Received net error:", err.Error())
		} else {
			l.LogError("Received unknown net error:", err.Error())
		}
	case error:
		switch err {
		case ErrTimeoutReadResponseBody:
			l.Stats.AddGetRecordsReadTimeout(1)
			l.LogError("Received error:", err.Error())
		default:
			l.LogError("Received error:", err.Error())
		}
	default:
		l.LogError("Received unknown error:", err.Error())
	}
}

// RetrieveWithContext waits for a message from the stream and returns the  Cancellation is supported through
// contexts.
func (l *Consumer) RetrieveWithContext(ctx context.Context) (*Message, error) {
	if !l.startConsuming() {
		return nil, ErrAlreadyConsuming
	}
	defer l.stopConsuming()

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		// A cancellation or closing the pipe of death will cause Retrieve (and related functions) to abort in
		// between getRecord calls.  Note, that this would only occur when there are no new records to retrieve.
		// Otherwise, getRecords will be allowed to run to completion and deliver one record.
		ok, err := l.shouldConsume(ctx)
		if !ok {
			return nil, err
		}
		n, _, err := l.enqueueSingle(childCtx)
		if n > 0 {
			l.Stats.AddDelivered(n)
			return <-l.messages, nil
		}
	}
}

// Retrieve waits for a message from the stream and returns the value.
func (l *Consumer) Retrieve() (*Message, error) {
	return l.RetrieveWithContext(context.TODO())
}

// RetrieveFnWithContext retrieves a message from the stream and dispatches it to the supplied function.  RetrieveFn
// will wait until the function completes. Cancellation is supported through context.
func (l *Consumer) RetrieveFnWithContext(ctx context.Context, fn MessageProcessor) error {
	msg, err := l.RetrieveWithContext(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		start := time.Now()
		fn(msg, &wg)
		l.Stats.AddProcessedDuration(time.Since(start))
		l.Stats.AddProcessed(1)
	}()

	return nil
}

// RetrieveFn retrieves a message from the stream and dispatches it to the supplied function.  RetrieveFn will wait
// until the function completes.
func (l *Consumer) RetrieveFn(fn MessageProcessor) error {
	return l.RetrieveFnWithContext(context.TODO(), fn)
}

// consume calls getRecords with configured batch size in a loop until the consumer is stopped.
func (l *Consumer) consume(ctx context.Context) {
	// We need to run startConsuming to make sure that we are okay and ready to start consuming.  This is mainly to
	// avoid a race condition where Listen() will attempt to read the messages channel prior to consume()
	// initializing it.  We can then launch a goroutine to handle the actual consume operation.
	if !l.startConsuming() {
		return
	}
	go func() {
		defer l.stopConsuming()

		// TODO: make these parameters configurable also scale according to the shard count
		l.txnCountRateLimiter = rate.NewLimiter(rate.Limit(5), 1)
		l.txSizeRateLimiter = rate.NewLimiter(rate.Limit(2000000), 2000000)

		childCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			// The consume loop can be cancelled by a calling the cancellation function on the context or by
			// closing the pipe of death.  Note that in the case of context cancellation, the getRecords
			// call below will be allowed to complete (as getRecords does not regard context cancellation).
			// In the case of cancellation by pipe of death, however, the getRecords will immediately abort
			// and allow the consume function to immediately abort as well.
			if ok, _ := l.shouldConsume(ctx); !ok {
				return
			}

			_, payloadSize, err := l.enqueueBatch(childCtx)
			if err != nil {
				l.LogError("Encountered an error when calling enqueueBatch: ", err)
				return
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()

				if err := l.txnCountRateLimiter.Wait(childCtx); err != nil {
					l.LogError("Error occured waiting for transaction count tokens")
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := l.txSizeRateLimiter.WaitN(childCtx, payloadSize); err != nil {
					l.LogError("Error occured waiting for transmission size tokens")
				}
			}()
			wg.Wait()
		}
	}()
}

// ListenWithContext listens and delivers message to the supplied function.  Upon cancellation, Listen will stop the
// consumer loop and wait until the messages channel is closed and all messages are delivered.
func (l *Consumer) ListenWithContext(ctx context.Context, fn MessageProcessor) {
	l.consume(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case msg, ok := <-l.messages:
			if !ok {
				return
			}
			l.Stats.AddDelivered(1)
			// For simplicity, did not do the pipe of death here. If POD is received, we may deliver a
			// couple more messages (especially since select is random in which channel is read from).
			l.concurrencySem <- empty{}
			wg.Add(1)
			go func(msg *Message) {
				defer func() {
					<-l.concurrencySem
				}()
				var fnWg sync.WaitGroup
				fnWg.Add(1)
				start := time.Now()
				fn(msg, &fnWg)
				fnWg.Wait()
				l.Stats.AddProcessedDuration(time.Since(start))
				l.Stats.AddProcessed(1)
				wg.Done()
			}(msg)
		case <-l.pipeOfDeath:
			l.LogInfo("ListenWithContext received pipe of death")
			return
		}
	}
}

// Listen listens and delivers message to the supplied function.
func (l *Consumer) Listen(fn MessageProcessor) {
	l.ListenWithContext(context.TODO(), fn)
}
