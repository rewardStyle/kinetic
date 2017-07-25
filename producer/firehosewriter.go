package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
)

type firehoseWriterOptions struct {
	msgCountRateLimit    int            // maximum number of records to be sent per second
	msgSizeRateLimit     int            // maximum (transmission) size of records to be sent per second
	throughputMultiplier int            // integer multiplier to increase firehose throughput rate limits
	Stats                StatsCollector // stats collection mechanism
}

// FirehoseWriter handles the API to send records to Kinesis.
type FirehoseWriter struct {
	*firehoseWriterOptions
	*logging.LogHelper

	stream string
	client firehoseiface.FirehoseAPI
}

// NewFirehoseWriter creates a new stream writer to write records to a Kinesis.
func NewFirehoseWriter(c *aws.Config, stream string, fn ...func(*FirehoseWriterConfig)) (*FirehoseWriter, error) {
	cfg := NewFirehoseWriterConfig(c)
	for _, f := range fn {
		f(cfg)
	}
	sess, err := session.NewSession(cfg.AwsConfig)
	if err != nil {
		return nil, err
	}
	return &FirehoseWriter{
		firehoseWriterOptions: cfg.firehoseWriterOptions,
		LogHelper: &logging.LogHelper{
			LogLevel: cfg.LogLevel,
			Logger:   cfg.AwsConfig.Logger,
		},
		stream: stream,
		client: firehose.New(sess),
	}, nil
}

// PutRecords sends a batch of records to Firehose and returns a list of records that need to be retried.
func (w *FirehoseWriter) PutRecords(ctx context.Context, messages []*message.Message, fn MessageHandlerAsync) error {
	var startSendTime time.Time
	var startBuildTime time.Time

	start := time.Now()
	var records []*firehose.Record
	for _, msg := range messages {
		if msg != nil {
			records = append(records, msg.ToFirehoseRecord())
		}
	}
	req, resp := w.client.PutRecordBatchRequest(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(w.stream),
		Records:            records,
	})

	req.Handlers.Build.PushFront(func(r *request.Request) {
		startBuildTime = time.Now()
		w.LogDebug("Start PutRecords Build, took", time.Since(start))
	})

	req.Handlers.Build.PushBack(func(r *request.Request) {
		w.Stats.UpdatePutRecordsBuildDuration(time.Since(startBuildTime))
		w.LogDebug("Finished PutRecords Build, took", time.Since(start))
	})

	req.Handlers.Send.PushFront(func(r *request.Request) {
		startSendTime = time.Now()
		w.LogDebug("Start PutRecords Send took", time.Since(start))
	})

	req.Handlers.Send.PushBack(func(r *request.Request) {
		w.Stats.UpdatePutRecordsSendDuration(time.Since(startSendTime))
		w.LogDebug("Finished PutRecords Send, took", time.Since(start))
	})

	w.LogDebug("Starting PutRecords Build/Sign request, took", time.Since(start))
	w.Stats.AddPutRecordsCalled(1)
	if err := req.Send(); err != nil {
		w.LogError("Error putting records:", err.Error())
		return err
	}
	w.Stats.UpdatePutRecordsDuration(time.Since(start))

	if resp == nil {
		return errs.ErrNilPutRecordsResponse
	}
	if resp.FailedPutCount == nil {
		return errs.ErrNilFailedRecordCount
	}
	attempted := len(messages)
	failed := int(aws.Int64Value(resp.FailedPutCount))
	sent := attempted - failed
	w.LogDebug(fmt.Sprintf("Finished PutRecords request, %d records attempted, %d records successful, %d records failed, took %v\n", attempted, sent, failed, time.Since(start)))

	for idx, record := range resp.RequestResponses {
		if record.RecordId != nil {
			// TODO: per-shard metrics
			messages[idx].RecordID = record.RecordId
			w.Stats.AddSentSuccess(1)
		} else {
			switch aws.StringValue(record.ErrorCode) {
			case firehose.ErrCodeLimitExceededException:
				w.Stats.AddProvisionedThroughputExceeded(1)
			default:
				w.LogDebug("PutRecords record failed with error:", aws.StringValue(record.ErrorCode), aws.StringValue(record.ErrorMessage))
			}
			messages[idx].ErrorCode = record.ErrorCode
			messages[idx].ErrorMessage = record.ErrorMessage
			messages[idx].FailCount++
			w.Stats.AddSentFailed(1)

			fn(messages[idx])
		}
	}

	return nil
}

// getMsgCountRateLimit returns the writer's message count rate limit
func (w *FirehoseWriter) getMsgCountRateLimit() int {
	return w.msgCountRateLimit
}

// getMsgSizeRateLimit returns the writer's message size rate limit
func (w *FirehoseWriter) getMsgSizeRateLimit() int {
	return w.msgSizeRateLimit
}

// getConcurrencyMultiplier returns the writer's concurrency multiplier.  For the firehosewriter the multiplier is 1.
func (w *FirehoseWriter) getConcurrencyMultiplier() (int, error) {
	return w.throughputMultiplier, nil
}
