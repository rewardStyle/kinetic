package kinesiswriter

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/producer"
)

var (
	// ErrNilPutRecordsResponse is returned when the PutRecords call returns
	// a nil response.
	ErrNilPutRecordsResponse = errors.New("PutRecords returned a nil response")

	// ErrNilFailedRecordCount is returned when the PutRecords call returns
	// a nil FailedRecordCount.
	ErrNilFailedRecordCount = errors.New("GetFailedRecordCount returned a nil FailedRecordCount")
)

type kinesisWriterOptions struct {
}

// KinesisWriter handles the API to send records to Kinesis.
type KinesisWriter struct {
	*kinesisWriterOptions

	producer *producer.Producer
	client   kinesisiface.KinesisAPI
	clientMu sync.Mutex
}

// NewKinesisWriter creates a new stream writer to write records to a Kinesis.
func NewKinesisWriter(fn func(*Config)) (*KinesisWriter, error) {
	config := NewConfig()
	fn(config)
	return &KinesisWriter{
		kinesisWriterOptions: config.kinesisWriterOptions,
	}, nil
}

// Log a debug message using the Producer logger.
func (w *KinesisWriter) Log(args ...interface{}) {
	w.producer.Log(args...)
}

// ensureClient will lazily make sure we have an AWS Kinesis client.
func (w *KinesisWriter) ensureClient() error {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	if w.client == nil {
		if w.producer == nil {
			return producer.ErrNilProducer
		}
		w.client = kinesis.New(w.producer.Session)
	}
	return nil
}

// AssociateProducer associates the Kinesis stream writer to a producer.
func (w *KinesisWriter) AssociateProducer(p *producer.Producer) error {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	if w.producer != nil {
		return producer.ErrProducerAlreadyAssociated
	}
	w.producer = p
	return nil
}

// PutRecords sends a batch of records to Kinesis and returns a list of records
// that need to be retried.
func (w *KinesisWriter) PutRecords(messages []*message.Message) ([]*message.Message, error) {
	if err := w.ensureClient(); err != nil {
		return nil, err
	}

	var startSendTime time.Time
	var startBuildTime time.Time

	start := time.Now()
	req, resp := w.client.PutRecordsRequest(&kinesis.PutRecordsInput{})

	req.Handlers.Build.PushFront(func(r *request.Request) {
		startBuildTime = time.Now()
		w.Log("Start PutRecords Build, took", time.Since(start))
	})

	req.Handlers.Build.PushBack(func(r *request.Request) {
		w.producer.Stats.AddPutRecordsBuildTime(time.Since(startBuildTime))
		w.Log("Finished PutRecords Build, took", time.Since(start))
	})

	req.Handlers.Send.PushFront(func(r *request.Request) {
		startSendTime = time.Now()
		w.Log("Start PutRecords Send took", time.Since(start))
	})

	req.Handlers.Build.PushBack(func(r *request.Request) {
		w.producer.Stats.AddPutRecordsSendTime(time.Since(startSendTime))
		w.Log("Finished PutRecords Send, took", time.Since(start))
	})

	w.Log("Starting PutRecords Build/Sign request, took", time.Since(start))
	w.producer.Stats.AddPutRecordsCalled(1)
	if err := req.Send(); err != nil {
		w.Log("Error putting records:", err)
		return nil, err
	}
	w.producer.Stats.AddPutRecordsTime(time.Since(start))

	if resp == nil {
		return nil, ErrNilPutRecordsResponse
	}
	if resp.FailedRecordCount == nil {
		return nil, ErrNilFailedRecordCount
	}
	attempted := len(messages)
	failed := int(aws.Int64Value(resp.FailedRecordCount))
	sent := attempted - failed
	w.producer.Stats.AddBatchSizeSample(len(messages))
	w.producer.Stats.AddSentSample(sent)
	w.producer.Stats.AddFailedSample(failed)
	w.Log(fmt.Sprintf("Finished PutRecords request, %d records attempted, %d records successful, %d records failed, took %v\n", attempted, sent, failed, time.Since(start)))

	var retries []*message.Message
	var err error
	for idx, record := range resp.Records {
		if record.SequenceNumber != nil && record.ShardId != nil {
			// TODO: per-shard metrics
		} else {
			// TODO metrics on failure rates
			retries = append(retries, messages[idx])
		}
	}
	if len(retries) > 0 {
		err = producer.ErrRetryRecords
	}
	return retries, err
}
