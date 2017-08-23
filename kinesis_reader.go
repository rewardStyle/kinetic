package kinetic

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	kinesisReaderMaxBatchSize = 10000
)

// kinesisReaderOptions a struct that holds all of the KinesisReader's configurable parameters.
type kinesisReaderOptions struct {
	batchSize           int                    // maximum records per GetRecordsRequest call
	shardIterator       *ShardIterator         // shard iterator for Kinesis stream
	responseReadTimeout time.Duration          // response read time out for GetRecordsRequest API call
	logLevel            aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats               ConsumerStatsCollector // stats collection mechanism
}

// defaultKinesisReaderOptions instantiates a kinesisReaderOptions with default values.
func defaultKinesisReaderOptions() *kinesisReaderOptions {
	return &kinesisReaderOptions{
		batchSize:           kinesisReaderMaxBatchSize,
		shardIterator:       NewShardIterator(),
		responseReadTimeout: time.Second,
		Stats:               &NilConsumerStatsCollector{},
	}
}

// KinesisReaderOptionsFn is a method signature for defining functional option methods for configuring
// the KinesisReader.
type KinesisReaderOptionsFn func(*KinesisReader) error

// KinesisReaderBatchSize is a functional option method for configuring the KinesisReader's
// batch size.
func KinesisReaderBatchSize(size int) KinesisReaderOptionsFn {
	return func(o *KinesisReader) error {
		if size > 0 && size <= kinesisReaderMaxBatchSize {
			o.batchSize = size
			return nil
		}
		return ErrInvalidBatchSize
	}
}

// KinesisReaderShardIterator is a functional option method for configuring the KinesisReader's
// shard iterator.
func KinesisReaderShardIterator(shardIterator *ShardIterator) KinesisReaderOptionsFn {
	return func(o *KinesisReader) error {
		o.shardIterator = shardIterator
		return nil
	}
}

// KinesisReaderResponseReadTimeout is a functional option method for configuring the KinesisReader's
// response read timeout.
func KinesisReaderResponseReadTimeout(timeout time.Duration) KinesisReaderOptionsFn {
	return func(o *KinesisReader) error {
		o.responseReadTimeout = timeout
		return nil
	}
}

// KinesisReaderLogLevel is a functional option method for configuring the KinesisReader's log level.
func KinesisReaderLogLevel(ll aws.LogLevelType) KinesisReaderOptionsFn {
	return func(o *KinesisReader) error {
		o.logLevel = ll & 0xffff0000
		return nil
	}
}

// KinesisReaderStats is a functional option method for configuring the KinesisReader's stats collector.
func KinesisReaderStats(sc ConsumerStatsCollector) KinesisReaderOptionsFn {
	return func(o *KinesisReader) error {
		o.Stats = sc
		return nil
	}
}

// KinesisReader handles the API to read records from Kinesis.
type KinesisReader struct {
	*kinesisReaderOptions
	*LogHelper
	stream            string
	shard             string
	throttleSem       chan empty
	nextShardIterator string
	client            kinesisiface.KinesisAPI
}

// NewKinesisReader creates a new KinesisReader object which implements the StreamReader interface to read records from
// Kinesis.
func NewKinesisReader(c *aws.Config, stream string, shard string, optionFns ...KinesisReaderOptionsFn) (*KinesisReader, error) {
	sess, err := session.NewSession(c)
	if err != nil {
		return nil, err
	}

	kinesisReader := &KinesisReader{
		kinesisReaderOptions: defaultKinesisReaderOptions(),
		stream:               stream,
		shard:                shard,
		throttleSem:          make(chan empty, 5),
		client:               kinesis.New(sess),
	}
	for _, optionFn := range optionFns {
		optionFn(kinesisReader)
	}

	kinesisReader.LogHelper = &LogHelper{
		LogLevel: kinesisReader.logLevel,
		Logger:   c.Logger,
	}

	return kinesisReader, nil
}

// ensureShardIterator will lazily make sure that we have a valid ShardIterator, calling the GetShardIterator API with
// the configured ShardIteratorType (with any applicable StartingSequenceNumber or Timestamp) if necessary.
//
// Not thread-safe.  Only called from getRecords Care must be taken to ensure that only one call to Listen and
// Retrieve/RetrieveFn can be running at a time.
func (r *KinesisReader) ensureShardIterator() error {
	if r.nextShardIterator != "" {
		return nil
	}

	resp, err := r.client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shard),                           // Required
		ShardIteratorType:      aws.String(r.shardIterator.shardIteratorType), // Required
		StreamName:             aws.String(r.stream),                          // Required
		StartingSequenceNumber: r.shardIterator.getStartingSequenceNumber(),
		Timestamp:              r.shardIterator.getTimestamp(),
	})
	if err != nil {
		r.LogError(err)
		return err
	}
	if resp == nil {
		return ErrNilGetShardIteratorResponse
	}
	if resp.ShardIterator == nil {
		return ErrNilShardIterator
	}
	return r.setNextShardIterator(*resp.ShardIterator)
}

// setNextShardIterator sets the nextShardIterator to use when calling GetRecords.
//
// Not thread-safe.  Only called from getRecords (and ensureShardIterator, which is called from getRecords).  Care must
// be taken to ensure that only one call to Listen and Retrieve/RetrieveFn can be running at a time.
func (r *KinesisReader) setNextShardIterator(shardIterator string) error {
	if len(shardIterator) == 0 {
		return ErrEmptyShardIterator
	}
	r.nextShardIterator = shardIterator
	return nil
}

// setSequenceNumber sets the sequenceNumber of shardIterator to the last delivered message and updates the
// shardIteratorType to AT_SEQUENCE_NUMBER. This is only used when we need to call getShardIterator (say, to refresh the
// shard iterator).
//
// Not thread-safe.  Only called from getRecords.  Care must be taken to ensure
// that only one call to Listen and Retrieve/RetrieveFn can be running at a
// time.
func (r *KinesisReader) setSequenceNumber(sequenceNumber string) error {
	if len(sequenceNumber) == 0 {
		return ErrEmptySequenceNumber
	}
	r.shardIterator.AtSequenceNumber(sequenceNumber)
	return nil
}

// Kinesis allows five read ops per second per shard.
// http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html
func (r *KinesisReader) throttle(sem chan empty) {
	sem <- empty{}
	time.AfterFunc(1*time.Second, func() {
		<-sem
	})
}

func (r *KinesisReader) getRecords(ctx context.Context, fn messageHandler, batchSize int) (count int, size int, err error) {
	if err = r.ensureShardIterator(); err != nil {
		r.LogError("Error calling ensureShardIterator(): ", err)
		return count, size, err
	}

	r.throttle(r.throttleSem)

	// We use the GetRecordsRequest method of creating requests to allow for registering custom handlers for better
	// control over the API request.
	var startReadTime time.Time
	var startUnmarshalTime time.Time
	start := time.Now()

	req, resp := r.client.GetRecordsRequest(&kinesis.GetRecordsInput{
		Limit:         aws.Int64(int64(batchSize)),
		ShardIterator: aws.String(r.nextShardIterator),
	})
	req.ApplyOptions(request.WithResponseReadTimeout(r.responseReadTimeout))

	// If debug is turned on, add some handlers for GetRecords logging
	if r.LogLevel.AtLeast(LogDebug) {
		req.Handlers.Send.PushBack(func(req *request.Request) {
			r.LogDebug("Finished getRecords Send, took", time.Since(start))
		})
	}

	// Here, we insert a handler to be called after the Send handler and before the the Unmarshal handler in the
	// aws-go-sdk library.
	//
	// The Send handler will call http.Client.Do() on the request, which blocks until the response headers have been
	// read before returning an HTTPResponse.
	//
	// The Unmarshal handler will ultimately call ioutil.ReadAll() on the HTTPResponse.Body stream.
	//
	// Our handler wraps the HTTPResponse.Body with our own ReadCloser so that we can implement stats collection
	req.Handlers.Unmarshal.PushFront(func(req *request.Request) {
		r.LogDebug("Started getRecords Unmarshal, took", time.Since(start))
		startReadTime = time.Now()

		req.HTTPResponse.Body = &ReadCloserWrapper{
			ReadCloser: req.HTTPResponse.Body,
			OnCloseFn: func() {
				r.Stats.UpdateGetRecordsReadResponseDuration(time.Since(startReadTime))
				r.LogDebug("Finished GetRecords body read, took", time.Since(start))
				startUnmarshalTime = time.Now()
			},
		}
	})

	req.Handlers.Unmarshal.PushBack(func(req *request.Request) {
		size += int(req.HTTPRequest.ContentLength)
		r.Stats.UpdateGetRecordsUnmarshalDuration(time.Since(startUnmarshalTime))
		r.LogDebug("Finished GetRecords Unmarshal, took", time.Since(start))
	})

	// Send the GetRecords request
	r.LogDebug("Starting GetRecords Build/Sign request, took", time.Since(start))
	r.Stats.AddGetRecordsCalled(1)
	if err = req.Send(); err != nil {
		r.LogError("Error getting records:", err)
		switch err.(awserr.Error).Code() {
		case kinesis.ErrCodeProvisionedThroughputExceededException:
			r.Stats.AddReadProvisionedThroughputExceeded(1)
		default:
			r.LogDebug("Received AWS error:", err.Error())
		}
		return count, size, err
	}
	r.Stats.UpdateGetRecordsDuration(time.Since(start))

	// Process Records
	r.LogDebug(fmt.Sprintf("Finished GetRecords request, %d records from shard %s, took %v\n", len(resp.Records), r.shard, time.Since(start)))
	if resp == nil {
		return count, size, ErrNilGetRecordsResponse
	}
	r.Stats.UpdateBatchSize(len(resp.Records))
	for _, record := range resp.Records {
		if record != nil {
			// Allow (only) a pipeOfDeath to trigger an instance shutdown of the loop to deliver messages.
			// Otherwise, a normal cancellation will not prevent getRecords from completing the delivery of
			// the current batch of records.
			select {
			case <-ctx.Done():
				r.LogInfo(fmt.Sprintf("getRecords received ctx.Done() while delivering messages, %d delivered, ~%d dropped", count, len(resp.Records)-count))
				return count, size, ctx.Err()
			default:
				fn(FromRecord(record))
				count++
				r.Stats.AddConsumed(1)
				if record.SequenceNumber != nil {
					// We can safely ignore if this call returns error, as if we somehow receive an
					// empty sequence number from AWS, we will simply not set it.  At worst, this
					// causes us to reprocess this record if we happen to refresh the iterator.
					r.setSequenceNumber(*record.SequenceNumber)
				}
			}
		}
	}
	if resp.NextShardIterator != nil {
		// TODO: According to AWS docs:
		// http://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#GetRecordsOutput
		//
		// NextShardIterator: The next position in the shard from which to start sequentially reading data
		// records.  If set to null, the shard has been closed and the requested iterator will not return any
		// more data.
		//
		// When dealing with streams that will merge or split, we need to detect that the shard has closed and
		// notify the client library.
		//
		// TODO: I don't know if we should be ignoring an error returned by setShardIterator in case of an empty
		// shard iterator in the response.  There isn't much we can do, and the best path for recovery may be
		// simply to reprocess the batch and see if we get a valid NextShardIterator from AWS the next time
		// around.
		r.setNextShardIterator(*resp.NextShardIterator)
	}
	return count, size, nil
}

// GetRecord calls getRecords and delivers one record into the messages channel.
func (r *KinesisReader) GetRecord(ctx context.Context, fn messageHandler) (count int, size int, err error) {
	count, size, err = r.getRecords(ctx, fn, 1)
	return count, size, err
}

// GetRecords calls getRecords and delivers each record into the messages channel.
func (r *KinesisReader) GetRecords(ctx context.Context, fn messageHandler) (count int, size int, err error) {
	count, size, err = r.getRecords(ctx, fn, r.batchSize)
	return count, size, err
}

// Checkpoint sends a message to KCL if there is sequence number that can be checkpointed
func (r *KinesisReader) Checkpoint() error {
	// No-op (only applicable to KclReader)
	return nil
}

// CheckpointInsert registers a sequence number with the checkpointer
func (r *KinesisReader) CheckpointInsert(seqNum string) error {
	// No-op (only applicable to KclReader)
	return nil
}

// CheckpointDone marks the given sequence number as done in the checkpointer
func (r *KinesisReader) CheckpointDone(seqNum string) error {
	// No-op (only applicable to KclReader)
	return nil
}
