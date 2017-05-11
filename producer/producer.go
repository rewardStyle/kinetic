package producer

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
)

// StreamWriter is an interface that abstracts the differences in API between
// Kinesis and Firehose.
type StreamWriter interface {
	AssociateProducer(producer *Producer) error
	PutRecords(message []*message.Message) ([]*message.Message, error)
	ensureClient() error
}

// Empty is used a as a dummy type for counting semaphore channels.
type Empty struct{}

type producerOptions struct {
	batchSize        int
	batchTimeout     time.Duration
	queueDepth       int
	maxRetryAttempts int
	concurrency      int
	writer           StreamWriter

	Stats StatsCollector
}

// Producer sends records to Kinesis or Firehose.
type Producer struct {
	*producerOptions
	*logging.LogHelper

	messages       chan *message.Message
	retries        chan *message.Message
	concurrencySem chan Empty
	pipeOfDeath    chan Empty
	outstanding    int
	shutdownCond   *sync.Cond
	producerWg     *sync.WaitGroup

	producing   bool
	producingMu sync.Mutex

	Session *session.Session
}

// NewProducer creates a new producer for writing records to a Kinesis or
// Firehose stream.
func NewProducer(fn func(*Config)) (*Producer, error) {
	config := NewConfig()
	fn(config)
	session, err := config.GetSession()
	if err != nil {
		return nil, err
	}
	p := &Producer{
		producerOptions: config.producerOptions,
		LogHelper: &logging.LogHelper{
			LogLevel: config.LogLevel,
			Logger:   session.Config.Logger,
		},
		concurrencySem: make(chan Empty, config.concurrency),
		pipeOfDeath:    make(chan Empty),
		Session:        session,
	}
	if err := p.writer.AssociateProducer(p); err != nil {
		return nil, err
	}
	return p, nil
}

// startConsuming will initialize the producer and set producing to true if
// there is not already another consume loop running.
func (p *Producer) startProducing() bool {
	p.producingMu.Lock()
	defer p.producingMu.Unlock()
	if !p.producing {
		p.producing = true
		p.messages = make(chan *message.Message, p.queueDepth)
		p.retries = make(chan *message.Message) // TODO: should we use a buffered channel?
		p.shutdownCond = &sync.Cond{L: &sync.Mutex{}}
		p.outstanding = 0
		return true
	}
	return false
}

// stopProducing handles any cleanup after a producing has stopped.
func (p *Producer) stopProducing() {
	p.producingMu.Lock()
	defer p.producingMu.Unlock()
	if p.messages != nil {
		close(p.messages)
	}
	p.producing = false
}

func (p *Producer) sendBatch(batch []*message.Message) {
	defer func() {
		p.shutdownCond.L.Lock()
		p.outstanding--
		p.shutdownCond.L.Unlock()
	}()
	attempts := 0
	var retries []*message.Message
	var err error
stop:
	for {
		retries, err = p.writer.PutRecords(batch)
		failed := len(retries)
		p.Stats.AddSent(len(batch) - failed)
		p.Stats.AddFailed(failed)
		if err == nil {
			break stop
		}
		switch err := err.(type) {
		case net.Error:
			if err.Timeout() {
				p.Stats.AddPutRecordsTimeout(1)
				p.LogError("Received net error:", err.Error())
			} else {
				p.LogError("Received unknown net error:", err.Error())
			}
		case awserr.Error:
			switch err.Code() {
			case kinesis.ErrCodeProvisionedThroughputExceededException:
				// FIXME: It is not clear to me whether
				// PutRecords would ever return a
				// ProvisionedThroughputExceeded error.  It
				// seems that it would instead return a valid
				// response in which some or all the records
				// within the response will contain an error
				// code and error message of
				// ProvisionedThroughputExceeded.  The current
				// assumption is that if we receive an
				// ProvisionedThroughputExceeded error, that the
				// entire batch should be retried.  Note we only
				// increment the PutRecord stat, instead of the
				// per-message stat.  Furthermore, we do not
				// increment the FailCount of the messages (as
				// the retry mechanism is different).
				p.Stats.AddPutRecordsProvisionedThroughputExceeded(1)
			default:
				p.LogError("Received AWS error:", err.Error())
			}
		case error:
			switch err {
			case errs.ErrRetryRecords:
				break stop
			default:
				p.LogError("Received error:", err.Error())
			}
		default:
			p.LogError("Received unknown error:", err.Error())
		}
		// NOTE: We may want to go through and increment the FailCount
		// for each of the records and allow the batch to be retried
		// rather than retrying the batch as-is.  With this approach, we
		// can kill the "stop" for loop, and set the entire batch to
		// retries to allow the below code to handle retrying the
		// messages.
		attempts++
		if attempts > p.maxRetryAttempts {
			p.LogError(fmt.Sprintf("Dropping batch after %d failed attempts to deliver to stream", attempts))
			p.Stats.AddDropped(len(batch))
			break stop
		}
	}
	// This frees up another sendBatch to run to allow drainage of the
	// messages / retry queue.  This should improve throughput as well as
	// prevent a potential deadlock in which all batches are blocked on
	// sending retries to the retries channel, and thus no batches are
	// allowed to drain the retry channel.
	<-p.concurrencySem
	for _, msg := range retries {
		if msg.FailCount < p.maxRetryAttempts {
			msg.FailCount++
			select {
			case p.retries <- msg:
			case <-p.pipeOfDeath:
				return
			}
		} else {
			p.Stats.AddDropped(1)
		}
	}
}

// produce calls the underlying writer's PutRecords implementation to deliver
// batches of messages to the target stream until the producer is stopped.
func (p *Producer) produce() {
	if !p.startProducing() {
		return
	}
	p.producerWg.Add(1)
	go func() {
		defer func() {
			p.stopProducing()
			p.producerWg.Done()
		}()
	stop:
		for {
			var batch []*message.Message
			timer := time.After(p.batchTimeout)
		batch:
			for len(batch) <= p.batchSize {
				select {
				// Using the select, retry messages will
				// interleave with new messages.  This is
				// preferable to putting the messages at the end
				// of the channel as it minimizes the delay in
				// the delivery of retry messages.
				case msg, ok := <-p.messages:
					if !ok {
						p.messages = nil
					} else {
						batch = append(batch, msg)
					}
				case msg := <-p.retries:
					batch = append(batch, msg)
				case <-timer:
					break batch
				case <-p.pipeOfDeath:
					break stop
				}
			}
			p.shutdownCond.L.Lock()
			if len(batch) > 0 {
				p.outstanding++
				p.Stats.AddBatchSize(len(batch))
				p.concurrencySem <- Empty{}
				go p.sendBatch(batch)
			} else if len(batch) == 0 {
				// We did not get any records -- check if we may
				// be (gracefully) shutting down the producer.
				// We can exit when:
				//   - The messages channel is nil and no new messages
				//     can be enqueued
				//   - There are no outstanding sendBatch goroutines
				//     and can therefore not produce any more messages
				//     to retry
				//   - The retry channel is empty
				if p.messages == nil && p.outstanding == 0 && len(p.retries) == 0 {
					close(p.retries)
					p.shutdownCond.Signal()
					break stop
				}
			}
			p.shutdownCond.L.Unlock()
		}
	}()
}

// CloseWithContext shuts down the producer, waiting for all outstanding
// messages and retries to flush.  Cancellation supported through contexts.
func (p *Producer) CloseWithContext(ctx context.Context) {
	c := make(chan Empty, 1)
	go func() {
		close(p.messages)
		p.shutdownCond.L.Lock()
		for p.outstanding != 0 {
			p.shutdownCond.Wait()
		}
		p.shutdownCond.L.Unlock()
		p.producerWg.Wait()
		c <- Empty{}
	}()
	select {
	case <-c:
	case <-ctx.Done():
		close(p.pipeOfDeath)
	}
}

// Close shuts down the producer, waiting for all outstanding messages and retries
// to flush.
func (p *Producer) Close() {
	p.CloseWithContext(context.TODO())
}

// SendWithContext sends a message to the stream.  Cancellation supported
// through contexts.
func (p *Producer) SendWithContext(ctx context.Context, msg *message.Message) error {
	p.produce()
	select {
	case p.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Send a message to the stream, waiting on the message to be put into the
// channel.
func (p *Producer) Send(msg *message.Message) error {
	return p.SendWithContext(context.TODO(), msg)
}

// TryToSend will attempt to send a message to the stream if the channel has
// capacity for a message, or will immediately return with an error if the
// channel is full.
func (p *Producer) TryToSend(msg *message.Message) error {
	ctx, cancel := context.WithTimeout(context.TODO(), 0*time.Second)
	defer cancel()
	return p.SendWithContext(ctx, msg)
}
