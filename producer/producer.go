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

	// ErrPipeOfDeath returns when the pipe of death is closed.
	ErrPipeOfDeath = errors.New("Received pipe of death")
)

// Empty is used a as a dummy type for counting semaphore channels.
type Empty struct{}

type producerOptions struct {
	batchSize    int
	batchTimeout time.Duration
	concurrency  int
	queueDepth   int
	writer       StreamWriter

	LogLevel aws.LogLevelType
	Stats    StatsCollector
}

// Producer sends records to Kinesis or Firehose.
type Producer struct {
	*producerOptions

	messages       chan *message.Message
	retries        chan []*message.Message
	concurrencySem chan Empty
	pipeOfDeath    chan Empty

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

// blockProducers will set producing to true if there is not already another
// produce loop running.
func (p *Producer) blockProducers() bool {
	p.producingMu.Lock()
	defer p.producingMu.Unlock()
	if !p.producing {
		p.producing = true
		return true
	}
	return false
}

// startProducing handles any initialization needed in preparation to start
// producing.
func (p *Producer) startProducing() {
	p.messages = make(chan *message.Message, p.queueDepth)
}

// shouldProduce is a convenience function that allows functions to break their
// loops if the context receives a cancellation.
func (p *Producer) shouldProduce(ctx context.Context) (bool, error) {
	select {
	case <-p.pipeOfDeath:
		return false, ErrPipeOfDeath
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		return true, nil
	}
}

// stopProducing handles any cleanup after a producing has stopped.
func (p *Producer) stopProducing() {
	close(p.messages)
}

// allowProducers allows producing.  Called after blockProducers to release the
// lock on producing.
func (p *Producer) allowProducers() {
	p.producingMu.Lock()
	defer p.producingMu.Unlock()
	p.producing = false
}

// IsProducing returns true while producing.
func (p *Producer) IsProducing() bool {
	p.producingMu.Lock()
	defer p.producingMu.Unlock()
	return p.producing
}

func (p *Producer) getBatch(ctx context.Context) ([]*message.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, p.batchTimeout)
	defer cancel()

	var batch []*message.Message
	select {
	case batch = <-p.retries:
	default:
	}

	for len(batch) < p.batchSize {
		select {
		case msg := <-p.messages:
			batch = append(batch, msg)
		case <-ctx.Done():
			return batch, ctx.Err()
		}
	}
	return batch, nil
}

func (p *Producer) produce(ctx context.Context) {
	if !p.blockProducers() {
		return
	}
	p.startProducing()
	go func() {
		defer func() {
			p.stopProducing()
			p.allowProducers()
		}()
	stop:
		for {
			ok, _ := p.shouldProduce(ctx)
			if !ok {
				break stop
			}
			batch, _ := p.getBatch(ctx)
			if len(batch) > 0 {
				p.concurrencySem <- Empty{}
				go func() {
					retries, err := p.writer.PutRecords(batch)
					<-p.concurrencySem
					if err != nil {
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
								p.retries <- retries
								p.Log("Received error:", err.Error())
							default:
								p.Log("Received error:", err.Error())
							}
						case awserr.Error:
							switch err.Code() {
							case kinesis.ErrCodeProvisionedThroughputExceededException:
								p.Stats.AddProvisionedThroughputExceeded(1)
							default:
								p.Log("Received AWS error:", err.Error())
							}
						default:
							p.Log("Received unknown error:", err.Error())
						}
					}
				}()
			}
		}
	}()
}

func (p *Producer) SendWithContext(ctx context.Context, msg *message.Message) error {
	select {
	case p.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Producer) Send(msg *message.Message) error {
	return p.SendWithContext(context.TODO(), msg)
}

func (p *Producer) TryToSend(msg *message.Message) error {
	ctx, cancel := context.WithTimeout(context.TODO(), 0*time.Second)
	defer cancel()
	return p.SendWithContext(ctx, msg)
}
