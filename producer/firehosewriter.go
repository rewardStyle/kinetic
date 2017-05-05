package producer

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/message"
)

// FirehoseWriter handles the API to send records to Kinesis.
type FirehoseWriter struct {
	stream string

	producer *Producer
	client   firehoseiface.FirehoseAPI
	clientMu sync.Mutex
}

// NewFirehoseWriter creates a new stream writer to write records to a Kinesis.
func NewFirehoseWriter(stream string) *FirehoseWriter {
	return &FirehoseWriter{
		stream: stream,
	}
}

// ensureClient will lazily make sure we have an AWS Kinesis client.
func (w *FirehoseWriter) ensureClient() error {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	if w.client == nil {
		if w.producer == nil {
			return errs.ErrNilProducer
		}
		w.client = firehose.New(w.producer.Session)
	}
	return nil
}

// AssociateProducer associates the Firehose stream writer to a producer.
func (w *FirehoseWriter) AssociateProducer(p *Producer) error {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	if w.producer != nil {
		return errs.ErrProducerAlreadyAssociated
	}
	w.producer = p
	return nil
}

// PutRecords sends a batch of records to Firehose and returns a list of records
// that need to be retried.
func (w *FirehoseWriter) PutRecords(messages []*message.Message) ([]*message.Message, error) {
	if err := w.ensureClient(); err != nil {
		return nil, err
	}

	var startSendTime time.Time
	var startBuildTime time.Time

	start := time.Now()
	var records []*firehose.Record
	for _, msg := range messages {
		records = append(records, msg.MakeFirehoseRecord())
	}
	req, resp := w.client.PutRecordBatchRequest(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(w.stream),
		Records:            records,
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
	if resp.FailedPutCount == nil {
		return nil, errs.ErrNilFailedRecordCount
	}
	attempted := len(messages)
	failed := int(aws.Int64Value(resp.FailedPutCount))
	sent := attempted - failed
	w.producer.LogDebug(fmt.Sprintf("Finished PutRecords request, %d records attempted, %d records successful, %d records failed, took %v\n", attempted, sent, failed, time.Since(start)))

	var retries []*message.Message
	var err error
	for idx, record := range resp.RequestResponses {
		if record.RecordId != nil {
			// TODO: per-shard metrics
			messages[idx].RecordID = record.RecordId
		} else {
			switch aws.StringValue(record.ErrorCode) {
			case firehose.ErrCodeLimitExceededException:
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
