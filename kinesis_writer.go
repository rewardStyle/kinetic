package kinetic

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	kinesisMsgCountRateLimit = 1000    // AWS Kinesis limit of 1000 records/sec
	kinesisMsgSizeRateLimit  = 1000000 // AWS Kinesis limit of 1 MB/sec
)

// kinesisWriterOptions is a struct that holds all of the KinesisWriter's configurable parameters.
type kinesisWriterOptions struct {
	responseReadTimeout time.Duration          // maximum time to wait for PutRecords API call before timing out
	msgCountRateLimit   int                    // maximum number of records to be sent per second
	msgSizeRateLimit    int                    // maximum (transmission) size of records to be sent per second
	logLevel            aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats               ProducerStatsCollector // stats collection mechanism
}

// defaultKinesisWriterOptions instantiates a kinesisWriterOptions with default values.
func defaultKinesisWriterOptions() *kinesisWriterOptions {
	return &kinesisWriterOptions{
		responseReadTimeout: time.Second,
		msgCountRateLimit:   kinesisMsgCountRateLimit,
		msgSizeRateLimit:    kinesisMsgSizeRateLimit,
		logLevel:            aws.LogOff,
		Stats:               &NilProducerStatsCollector{},
	}
}

// KinesisWriterOptionsFn is a method signature for defining functional option methods for configuring
// the KinesisWriter.
type KinesisWriterOptionsFn func(*KinesisWriter) error

// KinesisWriterResponseReadTimeout is a functional option method for configuring the KinesisWriter's
// response read timeout
func KinesisWriterResponseReadTimeout(timeout time.Duration) KinesisWriterOptionsFn {
	return func(o *KinesisWriter) error {
		o.responseReadTimeout = timeout
		return nil
	}
}

// KinesisWriterMsgCountRateLimit is a functional option method for configuring the KinesisWriter's
// message count rate limit
func KinesisWriterMsgCountRateLimit(limit int) KinesisWriterOptionsFn {
	return func(o *KinesisWriter) error {
		if limit > 0 && limit <= kinesisMsgCountRateLimit {
			o.msgSizeRateLimit = limit
			return nil
		}
		return ErrInvalidMsgSizeRateLimit
	}
}

// KinesisWriterMsgSizeRateLimit is a functional option method for configuring the KinesisWriter's
// message size rate limit
func KinesisWriterMsgSizeRateLimit(limit int) KinesisWriterOptionsFn {
	return func(o *KinesisWriter) error {
		if limit > 0 && limit <= kinesisMsgSizeRateLimit {
			o.msgSizeRateLimit = limit
			return nil
		}
		return ErrInvalidMsgSizeRateLimit
	}
}

// KinesisWriterLogLevel is a functional option method for configuring the KinesisWriter's log level
func KinesisWriterLogLevel(ll aws.LogLevelType) KinesisWriterOptionsFn {
	return func(o *KinesisWriter) error {
		o.logLevel = ll & 0xffff0000
		return nil
	}
}

// KinesisWriterStats is a functional option method for configuring the KinesisWriter's stats collector
func KinesisWriterStats(sc ProducerStatsCollector) KinesisWriterOptionsFn {
	return func(o *KinesisWriter) error {
		o.Stats = sc
		return nil
	}
}

// KinesisWriter handles the API to send records to Kinesis.
type KinesisWriter struct {
	*kinesisWriterOptions
	*LogHelper
	stream string
	client kinesisiface.KinesisAPI
}

// NewKinesisWriter creates a new stream writer to write records to a Kinesis.
func NewKinesisWriter(c *aws.Config, stream string, optionFns ...KinesisWriterOptionsFn) (*KinesisWriter, error) {
	sess, err := session.NewSession(c)
	if err != nil {
		return nil, err
	}

	kinesisWriter := &KinesisWriter{
		kinesisWriterOptions: defaultKinesisWriterOptions(),
		stream:               stream,
		client:               kinesis.New(sess),
	}
	for _, option := range optionFns {
		option(kinesisWriter)
	}

	kinesisWriter.LogHelper = &LogHelper{
		LogLevel: kinesisWriter.logLevel,
		Logger:   c.Logger,
	}

	return kinesisWriter, nil
}

// PutRecords sends a batch of records to Kinesis and returns a list of records that need to be retried.
func (w *KinesisWriter) PutRecords(ctx context.Context, messages []*Message, fn messageHandler) error {
	var startSendTime time.Time
	var startBuildTime time.Time

	start := time.Now()
	var records []*kinesis.PutRecordsRequestEntry
	for _, msg := range messages {
		if msg != nil {
			records = append(records, msg.ToRequestEntry())
		}
	}
	req, resp := w.client.PutRecordsRequest(&kinesis.PutRecordsInput{
		StreamName: aws.String(w.stream),
		Records:    records,
	})
	req.ApplyOptions(request.WithResponseReadTimeout(w.responseReadTimeout))

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
	if resp.FailedRecordCount == nil {
		return ErrNilFailedRecordCount
	}
	attempted := len(messages)
	failed := int(aws.Int64Value(resp.FailedRecordCount))
	sent := attempted - failed
	w.LogDebug(fmt.Sprintf("Finished PutRecords request, %d records attempted, %d records successful, %d records failed, took %v\n", attempted, sent, failed, time.Since(start)))

	for idx, record := range resp.Records {
		if record.SequenceNumber != nil && record.ShardId != nil {
			// TODO: per-shard metrics
			messages[idx].SequenceNumber = record.SequenceNumber
			messages[idx].ShardID = record.ShardId
			w.Stats.AddSentSuccess(1)
		} else {
			switch aws.StringValue(record.ErrorCode) {
			case kinesis.ErrCodeProvisionedThroughputExceededException:
				w.Stats.AddWriteProvisionedThroughputExceeded(1)
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
func (w *KinesisWriter) getMsgCountRateLimit() int {
	return w.msgCountRateLimit
}

// getMsgSizeRateLimit returns the writer's message size rate limit
func (w *KinesisWriter) getMsgSizeRateLimit() int {
	return w.msgSizeRateLimit
}

// getConcurrencyMultiplier returns the writer's concurrency multiplier.  For the kinesiswriter the multiplier is the
// number of active shards for the Kinesis stream
func (w *KinesisWriter) getConcurrencyMultiplier() (int, error) {
	resp, err := w.client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(w.stream),
	})
	if err != nil {
		w.LogError("Error describing kinesis stream: ", w.stream, err)
		return 0, err
	}
	if resp == nil {
		return 0, ErrNilDescribeStreamResponse
	}
	if resp.StreamDescription == nil {
		return 0, ErrNilStreamDescription
	}

	// maps shardID to a boolean that indicates whether or not the shard is a parent shard or an adjacent parent shard
	shardMap := make(map[string]bool)
	for _, shard := range resp.StreamDescription.Shards {
		if shard.ShardId != nil {
			shardID := aws.StringValue(shard.ShardId)
			if _, ok := shardMap[shardID]; !ok {
				shardMap[shardID] = false
			}
		}
	}

	// Loop through all the shards and mark which ones are parents
	for _, shard := range resp.StreamDescription.Shards {
		if shard.ParentShardId != nil {
			shardID := aws.StringValue(shard.ParentShardId)
			if _, ok := shardMap[shardID]; ok {
				shardMap[shardID] = true
			}
		}
		if shard.AdjacentParentShardId != nil {
			shardID := aws.StringValue(shard.AdjacentParentShardId)
			if _, ok := shardMap[shardID]; ok {
				shardMap[shardID] = true
			}
		}
	}

	// Determine the number of open shards by removing those shards that are reported as parents
	openShardCount := len(shardMap)
	for _, isParent := range shardMap {
		if isParent {
			openShardCount--
		}
	}

	return openShardCount, nil
}
