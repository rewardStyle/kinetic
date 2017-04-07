package producer

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
)

// StreamWriter is an interface that abstracts the differences in API between
// Kinesis and Firehose.
type StreamWriter interface {
	AssociateProducer(producer *Producer) error
	PutRecords(message []*message.Message) ([]*message.Message, error)
}

var (
	// ErrRetryRecords is returned when the PutRecords calls requires some
	// records of the batch to be retried.  This failure is considered part
	// of normal behavior of the Kinesis stream.
	ErrRetryRecords = errors.New("PutRecords requires retry of some records in batch")

	// ErrNilProducer is returned by a StreamWriter when it has not been
	// correctly associated with a Producer.
	ErrNilProducer = errors.New("StreamWriter not associated with a producer")

	// ErrProducerAlreadyAssociated is returned by a StreamWriter attempting
	// to associate it with a Producer when it already has an association
	// with a producer.
	ErrProducerAlreadyAssociated = errors.New("StreamWriter already associated with a producer")

	// ErrBatchTimeout is returned by getBatch whenever the batchTimeout
	// elapses prior to receiving batchSize messages.  This is *not* an
	// error in the sense of a failure, but is used to distinguish the
	// reason getBatch has exited.
	ErrBatchTimeout = errors.New("A timeout has occurred before batch has reached optimal size")

	// ErrProducerShutdown is returend by getBatch whenever both the message
	// and retry channel have been closed.
	ErrProducerShutdown = errors.New("The producer has shut down")

	// ErrPipeOfDeath returns when the pipe of death is closed.
	ErrPipeOfDeath = errors.New("Received pipe of death")
)

// Empty is used a as a dummy type for counting semaphore channels.
type Empty struct{}

type producerOptions struct {
	batchSize        int
	batchTimeout     time.Duration
	queueDepth       int
	maxRetryAttempts int
	concurrency      int
	writer           StreamWriter

	LogLevel aws.LogLevelType
	Stats    StatsCollector
}

// Producer sends records to Kinesis or Firehose.
type Producer struct {
	*producerOptions

	messages       chan *message.Message
	retries        chan *message.Message
	concurrencySem chan Empty
	pipeOfDeath    chan Empty
	producerWg     sync.WaitGroup

	outstanding int64
	flushCond   sync.Cond

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
		concurrencySem:  make(chan Empty, config.concurrency),
		pipeOfDeath:     make(chan Empty),
		Session:         session,
	}
	if err := p.writer.AssociateProducer(p); err != nil {
		return nil, err
	}
	return p, nil
}

// Log a debug message using the AWS SDK logger.
func (p *Producer) Log(args ...interface{}) {
	if p.Session.Config.LogLevel.Matches(logging.LogDebug) {
		p.Session.Config.Logger.Log(args...)
	}
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
		p.outstanding = 0
		p.flushCond = sync.Cond{L: &sync.Mutex{}}
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

// getBatch will retrieve a batch of messages by batchSize and batchTimeout for
// delivery.
func (p *Producer) getBatch() ([]*message.Message, error) {
	var err error
	var batch []*message.Message
	var timer <-chan time.Time
stop:
	for len(batch) <= p.batchSize {
		select {
		// Using the select, retry messages will interleave with new
		// messages.  This is preferable to putting the messages at the
		// end of the channel as it minimizes the delay in the delivery
		// of retry messages.
		case msg, ok := <-p.retries:
			if !ok {
				p.retries = nil
			} else {
				batch = append(batch, msg)
				if timer != nil {
					timer = time.After(p.batchTimeout)
				}
			}
		case msg, ok := <-p.messages:
			if !ok {
				p.messages = nil
			} else {
				batch = append(batch, msg)
				if timer != nil {
					timer = time.After(p.batchTimeout)
				}
			}
		case <-timer:
			err = ErrBatchTimeout
			break stop
		case <-p.pipeOfDeath:
			return nil, ErrPipeOfDeath
		}
		if p.messages == nil && p.retries == nil {
			err = ErrProducerShutdown
			break stop
		}
	}
	p.Stats.AddBatchSizeSample(len(batch))
	return batch, err
}

func (p *Producer) dispatchBatch(batch []*message.Message) {
	defer p.flushCond.Signal()
stop:
	for {
		retries, err := p.writer.PutRecords(batch)
		failed := len(retries)
		sent := len(batch) - failed
		p.Stats.AddSentSample(sent)
		p.Stats.AddFailedSample(failed)
		p.decOutstanding(int64(sent))
		// This frees up another dispatchBatch to run to allow drainage
		// of the messages / retry queue.  This should improve
		// throughput as well as prevent a potential deadlock in which
		// all batches are blocked on sending retries to the retries
		// channel, and thus no batches are allowed to drain the retry
		// channel.
		<-p.concurrencySem
		if err == nil {
			break stop
		}
		switch err := err.(type) {
		case net.Error:
			if err.Timeout() {
				p.Stats.AddPutRecordsTimeout(1)
				p.Log("Received net error:", err.Error())
			} else {
				p.Log("Received unknown net error:", err.Error())
			}
		case error:
			switch err {
			case ErrRetryRecords:
				for _, msg := range retries {
					if msg.FailCount < p.maxRetryAttempts {
						msg.FailCount++
						select {
						case p.retries <- msg:
						case <-p.pipeOfDeath:
							break stop
						}
					} else {
						p.decOutstanding(1)
						p.Stats.AddDroppedSample(1)
					}
				}
				p.Log("Received error:", err.Error())
			default:
				p.Log("Received error:", err.Error())
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
				p.Log("Received AWS error:", err.Error())
			}
		default:
			p.Log("Received unknown error:", err.Error())
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
			batch, err := p.getBatch()
			// If getBatch aborted due to pipe of death, we will
			// immediately exit the loop.
			if err == ErrPipeOfDeath {
				break stop
			}
			// Regardless if getBatch produced an error (as long as
			// its not the pipe of death), we will send the messages
			// via PutRecords.
			if len(batch) > 0 {
				p.concurrencySem <- Empty{}
				go p.dispatchBatch(batch)
			}
			// If we exited getBatch earlier with a
			// ErrProducerShutdown we shut down the producer.
			if err == ErrProducerShutdown {
				break stop
			}
		}
	}()
}

// incOutstanding increments the number of outstanding messages that are to be
// delivered.
func (p *Producer) incOutstanding(i int64) {
	p.flushCond.L.Lock()
	defer p.flushCond.L.Unlock()
	p.outstanding += i
}

// decOutstanding decrements the number of outstanding messages that are to be
// delivered.
func (p *Producer) decOutstanding(i int64) {
	p.flushCond.L.Lock()
	defer p.flushCond.L.Unlock()
	p.outstanding -= i
}

// Close shuts down the producer, waiting for all outstanding messages and retries
// to flush.
func (p *Producer) Close() {
	close(p.messages)
	p.flushCond.L.Lock()
	for p.outstanding != 0 {
		p.flushCond.Wait()
	}
	close(p.retries)
	p.flushCond.L.Unlock()
	p.producerWg.Wait()
}

// SendWithContext sends a message to the stream.  Cancellation supported
// through contexts.
func (p *Producer) SendWithContext(ctx context.Context, msg *message.Message) error {
	p.produce()
	select {
	case p.messages <- msg:
		p.incOutstanding(1)
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
