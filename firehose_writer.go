package kinetic

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
)

const (
	firehoseMsgCountRateLimit = 5000    // AWS Firehose limit of 5000 records/sec
	firehoseMsgSizeRateLimit  = 5000000 // AWS Firehose limit of 5 MB/sec
)

// firehoseWriterOptions is a struct that holds all of the FirehoseWriter's configurable parameters.
type firehoseWriterOptions struct {
	msgCountRateLimit    int                    // maximum number of records to be sent per second
	msgSizeRateLimit     int                    // maximum (transmission) size of records to be sent per second
	throughputMultiplier int                    // integer multiplier to increase firehose throughput rate limits
	logLevel             aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats                ProducerStatsCollector // stats collection mechanism
}

// defaultFirehoseWriterOptions instantiates a firehoseWriterOptions with default values.
func defaultFirehoseWriterOptions() *firehoseWriterOptions {
	return &firehoseWriterOptions{
		msgCountRateLimit:    firehoseMsgCountRateLimit,
		msgSizeRateLimit:     firehoseMsgSizeRateLimit,
		throughputMultiplier: 1,
		logLevel:             aws.LogOff,
		Stats:                &NilProducerStatsCollector{},
	}
}

// FireHoseWriterOptionsFn is a method signature for defining functional option methods for configuring
// the FirehoseWriter.
type FireHoseWriterOptionsFn func(*firehoseWriterOptions) error

// FirehoseWriterMsgCountRateLimit is a functional option method for configuring the FirehoseWriter's
// message count rate limit.
func FirehoseWriterMsgCountRateLimit(limit int) FireHoseWriterOptionsFn {
	return func(o *firehoseWriterOptions) error {
		if limit > 0 && limit <= firehoseMsgCountRateLimit {
			o.msgCountRateLimit = limit
			return nil
		}
		return ErrInvalidMsgCountRateLimit
	}
}

// FirehoseWriterMsgSizeRateLimit is a functional option method for configuring the FirehoseWriter's
// messsage size rate limit.
func FirehoseWriterMsgSizeRateLimit(limit int) FireHoseWriterOptionsFn {
	return func(o *firehoseWriterOptions) error {
		if limit > 0 && limit <= firehoseMsgSizeRateLimit {
			o.msgSizeRateLimit = limit
			return nil
		}
		return ErrInvalidMsgSizeRateLimit
	}
}

// FirehoseWriterThroughputMultiplier is a functional option method for configuring the FirehoseWriter's
// throughput multiplier.
func FirehoseWriterThroughputMultiplier(multiplier int) FireHoseWriterOptionsFn {
	return func(o *firehoseWriterOptions) error {
		if multiplier > 0 {
			o.throughputMultiplier = multiplier
			return nil
		}
		return ErrInvalidThroughputMultiplier
	}
}

// FirehoseWriterLogLevel is a functional option method for configuring the FirehoseWriter's log level.
func FirehoseWriterLogLevel(ll aws.LogLevelType) FireHoseWriterOptionsFn {
	return func(o *firehoseWriterOptions) error {
		o.logLevel = ll & 0xffff0000
		return nil
	}
}

// FirehoseWriterStats is a functional option method for configuring the FirehoseWriter's stats collector.
func FirehoseWriterStats(sc ProducerStatsCollector) FireHoseWriterOptionsFn {
	return func(o *firehoseWriterOptions) error {
		o.Stats = sc
		return nil
	}
}

// FirehoseWriter handles the API to send records to Kinesis.
type FirehoseWriter struct {
	*firehoseWriterOptions
	*LogHelper
	stream string
	client firehoseiface.FirehoseAPI
}

// NewFirehoseWriter creates a new stream writer to write records to a Kinesis.
func NewFirehoseWriter(c *aws.Config, stream string, optionFns ...FireHoseWriterOptionsFn) (*FirehoseWriter, error) {
	firehoseWriterOptions := defaultFirehoseWriterOptions()
	for _, optionFn := range optionFns {
		optionFn(firehoseWriterOptions)
	}
	sess, err := session.NewSession(c)
	if err != nil {
		return nil, err
	}
	return &FirehoseWriter{
		stream: stream,
		client: firehose.New(sess),
		firehoseWriterOptions: firehoseWriterOptions,
		LogHelper: &LogHelper{
			LogLevel: firehoseWriterOptions.logLevel,
			Logger:   c.Logger,
		},
	}, nil
}

// PutRecords sends a batch of records to Firehose and returns a list of records that need to be retried.
func (w *FirehoseWriter) PutRecords(ctx context.Context, messages []*Message, fn MessageHandlerAsync) error {
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
		return ErrNilPutRecordsResponse
	}
	if resp.FailedPutCount == nil {
		return ErrNilFailedRecordCount
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
				w.Stats.AddPutRecordsProvisionedThroughputExceeded(1)
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
