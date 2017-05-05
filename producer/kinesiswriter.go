package producer

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/message"
)

// KinesisWriter handles the API to send records to Kinesis.
type KinesisWriter struct {
	stream string

	producer *Producer
	client   kinesisiface.KinesisAPI
	clientMu sync.Mutex
}

// NewKinesisWriter creates a new stream writer to write records to a Kinesis.
func NewKinesisWriter(stream string) *KinesisWriter {
	return &KinesisWriter{
		stream: stream,
	}
}

// ensureClient will lazily make sure we have an AWS Kinesis client.
func (w *KinesisWriter) ensureClient() error {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	if w.client == nil {
		if w.producer == nil {
			return errs.ErrNilProducer
		}
		w.client = kinesis.New(w.producer.Session)
	}
	return nil
}

// AssociateProducer associates the Kinesis stream writer to a producer.
func (w *KinesisWriter) AssociateProducer(p *Producer) error {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	if w.producer != nil {
		return errs.ErrProducerAlreadyAssociated
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
	var records []*kinesis.PutRecordsRequestEntry
	for _, msg := range messages {
		records = append(records, msg.MakeRequestEntry())
	}
	req, resp := w.client.PutRecordsRequest(&kinesis.PutRecordsInput{
		StreamName: aws.String(w.stream),
		Records:    records,
	})

	req.Handlers.Build.PushFront(func(r *request.Request) {
		startBuildTime = time.Now()
		w.producer.LogDebug("Start PutRecords Build, took", time.Since(start))
	})

	req.Handlers.Build.PushBack(func(r *request.Request) {
		w.producer.Stats.AddPutRecordsBuildDuration(time.Since(startBuildTime))
		w.producer.LogDebug("Finished PutRecords Build, took", time.Since(start))
	})

	req.Handlers.Send.PushFront(func(r *request.Request) {
		startSendTime = time.Now()
		w.producer.LogDebug("Start PutRecords Send took", time.Since(start))
	})

	req.Handlers.Build.PushBack(func(r *request.Request) {
		w.producer.Stats.AddPutRecordsSendDuration(time.Since(startSendTime))
		w.producer.LogDebug("Finished PutRecords Send, took", time.Since(start))
	})

	w.producer.LogDebug("Starting PutRecords Build/Sign request, took", time.Since(start))
	w.producer.Stats.AddPutRecordsCalled(1)
	if err := req.Send(); err != nil {
		w.producer.LogError("Error putting records:", err.Error())
		return nil, err
	}
	w.producer.Stats.AddPutRecordsDuration(time.Since(start))

	if resp == nil {
		return nil, errs.ErrNilPutRecordsResponse
	}
	if resp.FailedRecordCount == nil {
		return nil, errs.ErrNilFailedRecordCount
	}
	attempted := len(messages)
	failed := int(aws.Int64Value(resp.FailedRecordCount))
	sent := attempted - failed
	w.producer.LogDebug(fmt.Sprintf("Finished PutRecords request, %d records attempted, %d records successful, %d records failed, took %v\n", attempted, sent, failed, time.Since(start)))

	var retries []*message.Message
	var err error
	for idx, record := range resp.Records {
		if record.SequenceNumber != nil && record.ShardId != nil {
			// TODO: per-shard metrics
			messages[idx].SequenceNumber = record.SequenceNumber
			messages[idx].ShardID = record.ShardId
		} else {
			switch aws.StringValue(record.ErrorCode) {
			case kinesis.ErrCodeProvisionedThroughputExceededException:
				w.producer.Stats.AddProvisionedThroughputExceeded(1)
			default:
				w.producer.LogDebug("PutRecords record failed with error:", aws.StringValue(record.ErrorCode), aws.StringValue(record.ErrorMessage))
			}
			messages[idx].ErrorCode = record.ErrorCode
			messages[idx].ErrorMessage = record.ErrorMessage
			messages[idx].FailCount++
			retries = append(retries, messages[idx])
		}
	}
	if len(retries) > 0 {
		err = errs.ErrRetryRecords
	}
	return retries, err
}
