package consumer

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic"
	"golang.org/x/time/rate"
)

// consumerOptions is used to hold all of the configurable settings of a Listener object.
type consumerOptions struct {
	queueDepth  int
	concurrency int
	logLevel    aws.LogLevelType // log level for configuring the LogHelper's log level
	Stats       StatsCollector   // stats collection mechanism
}

func defaultConsumerOptions() *consumerOptions {
	return &consumerOptions{
		queueDepth:  10000,
		concurrency: 10,
		Stats:       &NilStatsCollector{},
	}
}

type ConsumerOptionsFn func(*consumerOptions) error

func ConsumerQueueDepth(depth int) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		if depth > 0 {
			o.queueDepth = depth
			return nil
		}
		return kinetic.ErrInvalidQueueDepth
	}
}

func ConsumerConcurrency(count int) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		if count > 0 {
			o.concurrency = count
			return nil
		}
		return kinetic.ErrInvalidConcurrency
	}
}

func ConsumerLogLevel(ll aws.LogLevelType) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		o.logLevel = ll & 0xffff0000
		return nil
	}
}

func ConsumerStatsCollector(sc StatsCollector) ConsumerOptionsFn {
	return func(o *consumerOptions) error {
		o.Stats = sc
		return nil
	}
}

// Listener polls the StreamReader for messages.
type Consumer struct {
	*consumerOptions
	*kinetic.LogHelper
	reader              StreamReader
	txnCountRateLimiter *rate.Limiter
	txSizeRateLimiter   *rate.Limiter
	messages            chan *kinetic.Message
	concurrencySem      chan empty
	pipeOfDeath         chan empty
	consuming           bool
	consumingMu         sync.Mutex
}

// NewListener creates a new Listener object for retrieving and listening to message(s) on a StreamReader.
func NewConsumer(c *aws.Config, r StreamReader, optionFns ...ConsumerOptionsFn) (*Consumer, error) {
	consumerOptions := defaultConsumerOptions()
	for _, optionFn := range optionFns {
		optionFn(consumerOptions)
	}
	return &Consumer{
		consumerOptions: consumerOptions,
		LogHelper: &kinetic.LogHelper{
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
		l.messages = make(chan *kinetic.Message, l.queueDepth)
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
		return false, kinetic.ErrPipeOfDeath
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

func (l *Consumer) enqueueSingle(ctx context.Context) (int, int, error) {
	n, m, err := l.reader.GetRecord(ctx, func(msg *kinetic.Message, wg *sync.WaitGroup) error {
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

func (l *Consumer) enqueueBatch(ctx context.Context) (int, int, error) {
	n, m, err := l.reader.GetRecords(ctx,
		func(msg *kinetic.Message, wg *sync.WaitGroup) error {
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
		case kinetic.ErrTimeoutReadResponseBody:
			l.Stats.AddGetRecordsReadTimeout(1)
			l.LogError("Received error:", err.Error())
		default:
			l.LogError("Received error:", err.Error())
		}
	default:
		l.LogError("Received unknown error:", err.Error())
	}
}

// RetrieveWithContext waits for a message from the stream and returns the kinetic. Cancellation is supported through
// contexts.
func (l *Consumer) RetrieveWithContext(ctx context.Context) (*kinetic.Message, error) {
	if !l.startConsuming() {
		return nil, kinetic.ErrAlreadyConsuming
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

// Retrieve waits for a message from the stream and returns the value
func (l *Consumer) Retrieve() (*kinetic.Message, error) {
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

// consume calls getRecords with configured batch size in a loop until the listener is stopped.
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
			go func(msg *kinetic.Message) {
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
