package listener

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
)

// StreamReader is an interface that abstracts out a stream reader
type StreamReader interface {
	AssociateListener(listener *Listener) error
	GetRecords() (int, error)
	GetNRecords(batchSize int) (int, error)
	ensureClient() error
}

// Empty is used a as a dummy type for counting semaphore channels.
type Empty struct{}

// MessageFn defines the signature of a message handler used by Listen and
// RetrieveFn.
type MessageFn func([]byte, *sync.WaitGroup)

type listenerOptions struct {
	queueDepth            int
	concurrency           int
	getRecordsReadTimeout time.Duration
	reader                StreamReader
	Stats 		      StatsCollector
}

// Listener polls the StreamReader for messages.
type Listener struct {
	*listenerOptions
	*logging.LogHelper

	messages       chan *message.Message
	concurrencySem chan Empty
	pipeOfDeath    chan Empty

	consuming      bool
	consumingMu    sync.Mutex

	Session        session.Session
}

// NewListener creates a new listener for listening to message on a StreamReader.
func NewListener(fn func(*Config)) (*Listener, error) {
	config := NewConfig()
	fn(config)
	session, err := config.GetSession()
	if err != nil {
		return nil, err
	}
	l := &Listener{
		listenerOptions: config.listenerOptions,
		LogHelper: &logging.LogHelper{
			LogLevel: config.LogLevel,
			Logger:   session.Config.Logger,
		},
		concurrencySem: make(chan Empty, config.concurrency),
		pipeOfDeath:    make(chan Empty),
		Session: session,
	}
	if err := l.reader.AssociateListener(l); err != nil {
		return nil, err
	}
	return l, nil
}

// startConsuming will initialize the consumer and set consuming to true if
// there is not already another consume loop running.
func (l *Listener) startConsuming() bool {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	if !l.consuming {
		l.consuming = true
		l.messages = make(chan *message.Message, l.queueDepth)
		return true
	}
	return false
}

// shouldConsume is a convenience function that allows functions to break their
// loops if the context receives a cancellation.
func (l *Listener) shouldConsume(ctx context.Context) (bool, error) {
	select {
	case <-l.pipeOfDeath:
		return false, errs.ErrPipeOfDeath
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		return true, nil
	}
}

// stopConsuming handles any cleanup after a consuming has stopped.
func (l *Listener) stopConsuming() {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	if l.messages != nil {
		close(l.messages)
	}
	l.consuming = false
}

// RetrieveWithContext waits for a message from the stream and return the value.
// Cancellation supported through contexts.
func (l *Listener) RetrieveWithContext(ctx context.Context) (*message.Message, error) {
	if !l.startConsuming() {
		return nil, errs.ErrAlreadyConsuming
	}
	defer func() {
		l.stopConsuming()
	}()
	for {
		// A cancellation or closing the pipe of death will cause
		// Retrieve (and related functions) to abort in between
		// getRecord calls.  Note, that this would only occur when there
		// are no new records to retrieve.  Otherwise, getRecords will
		// be allowed to run to completion and deliver one record.
		ok, err := l.shouldConsume(ctx)
		if !ok {
			return nil, err
		}
		n, err := l.reader.GetNRecords(1)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			l.Stats.AddDelivered(1)
			return <-l.messages, nil
		}
	}
}

// Retrieve waits for a message from the stream and return the value.
func (l *Listener) Retrieve() (*message.Message, error) {
	return l.RetrieveWithContext(context.TODO())
}

// RetrieveFnWithContext retrieves a message from the stream and dispatches it
// to the supplied function.  RetrieveFn will wait until the function completes.
// Cancellation supported through context.
func (l *Listener) RetrieveFnWithContext(ctx context.Context, fn MessageFn) error {
	msg, err := l.RetrieveWithContext(ctx)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	start := time.Now()
	go fn(msg.Data, &wg)
	wg.Wait()
	l.Stats.AddProcessedDuration(time.Since(start))
	l.Stats.AddProcessed(1)
	return nil
}

// RetrieveFn retrieves a message from the stream and dispatches it to the
// supplied function.  RetrieveFn will wait until the function completes.
func (l *Listener) RetrieveFn(fn MessageFn) error {
	return l.RetrieveFnWithContext(context.TODO(), fn)
}

// consume calls getRecords with configured batch size in a loop until the
// listener is stopped.
func (l *Listener) consume(ctx context.Context) {
	// We need to run startConsuming to make sure that we are okay and ready
	// to start consuming.  This is mainly to avoid a race condition where
	// Listen() will attempt to read the messages channel prior to consume()
	// initializing it.  We can then launch a goroutine to handle the actual
	// consume operation.
	if !l.startConsuming() {
		return
	}
	go func() {
		defer func() {
			l.stopConsuming()
		}()
	stop:
		for {
			// The consume loop can be cancelled by a calling the
			// cancellation function on the context or by closing
			// the pipe of death.  Note that in the case of context
			// cancellation, the getRecords call below will be
			// allowed to complete (as getRecords does not regard
			// context cancellation).  In the case of cancellation
			// by pipe of death, however, the getRecords will
			// immediately abort and allow the consume function to
			// immediately abort as well.
			ok, _ := l.shouldConsume(ctx)
			if !ok {
				break stop
			}
			_, err := l.reader.GetRecords()
			if err != nil {
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
					case errs.ErrTimeoutReadResponseBody:
						l.Stats.AddGetRecordsReadTimeout(1)
						l.LogError("Received error:", err.Error())
					default:
						l.LogError("Received error:", err.Error())
					}
				default:
					l.LogError("Received unknown error:", err.Error())
				}
			}
		}
	}()
}

// ListenWithContext listens and delivers message to the supplied function.
// Upon cancellation, Listen will stop the consumer loop and wait until the
// messages channel is closed and all messages are delivered.
func (l *Listener) ListenWithContext(ctx context.Context, fn MessageFn) {
	l.consume(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()

stop:
	for {
		select {
		case msg, ok := <-l.messages:
			if !ok {
				break stop
			}
			l.Stats.AddDelivered(1)
			// For simplicity, did not do the pipe of death here.
			// If POD is received, we may deliver a couple more
			// messages (especially since select is random in which
			// channel is read from).
			l.concurrencySem <- Empty{}
			wg.Add(1)
			go func(msg *message.Message) {
				defer func() {
					<-l.concurrencySem
				}()
				var fnWg sync.WaitGroup
				fnWg.Add(1)
				start := time.Now()
				fn(msg.Data, &fnWg)
				fnWg.Wait()
				l.Stats.AddProcessedDuration(time.Since(start))
				l.Stats.AddProcessed(1)
				wg.Done()
			}(msg)
		case <-l.pipeOfDeath:
			l.LogInfo("ListenWithContext received pipe of death")
			break stop
		}
	}
}

// Listen listens and delivers message to the supplied function.
func (l *Listener) Listen(fn MessageFn) {
	l.ListenWithContext(context.TODO(), fn)
}
