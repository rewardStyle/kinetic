package listener

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
)

// kinesisReaderOptions is used to hold all of the configurable settings of a KinesisReader.
type kinesisReaderOptions struct {
	batchSize           int
	shardIterator       *ShardIterator
	responseReadTimeout time.Duration
	Stats               StatsCollector
}

// KinesisReader handles the API to read records from Kinesis.
type KinesisReader struct {
	*kinesisReaderOptions
	*logging.LogHelper
	stream            string
	shard             string
	throttleSem       chan empty
	nextShardIterator string
	client            kinesisiface.KinesisAPI
}

// NewKinesisReader creates a new KinesisReader object which implements the StreamReader interface to read records from
// Kinesis.
func NewKinesisReader(c *aws.Config, stream string, shard string, fn ...func(*KinesisReaderConfig)) (*KinesisReader, error) {
	cfg := NewKinesisReaderConfig(c)
	for _, f := range fn {
		f(cfg)
	}
	sess, err := session.NewSession(cfg.AwsConfig)
	if err != nil {
		return nil, err
	}
	return &KinesisReader{
		kinesisReaderOptions: cfg.kinesisReaderOptions,
		LogHelper: &logging.LogHelper{
			LogLevel: cfg.LogLevel,
			Logger:   cfg.AwsConfig.Logger,
		},
		stream:      stream,
		shard:       shard,
		throttleSem: make(chan empty, 5),
		client:      kinesis.New(sess),
	}, nil
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
		return errs.ErrNilGetShardIteratorResponse
	}
	if resp.ShardIterator == nil {
		return errs.ErrNilShardIterator
	}
	return r.setNextShardIterator(*resp.ShardIterator)
}

// setNextShardIterator sets the nextShardIterator to use when calling GetRecords.
//
// Not thread-safe.  Only called from getRecords (and ensureShardIterator, which is called from getRecords).  Care must
// be taken to ensure that only one call to Listen and Retrieve/RetrieveFn can be running at a time.
func (r *KinesisReader) setNextShardIterator(shardIterator string) error {
	if len(shardIterator) == 0 {
		return errs.ErrEmptyShardIterator
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
		return errs.ErrEmptySequenceNumber
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

func (r *KinesisReader) getRecords(ctx context.Context, fn MessageHandler, batchSize int) (int, int, error) {
	if err := r.ensureShardIterator(); err != nil {
		return 0, 0, err
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
	if r.LogLevel.AtLeast(logging.LogDebug) {
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
				r.Stats.AddGetRecordsReadResponseDuration(time.Since(startReadTime))
				r.LogDebug("Finished GetRecords body read, took", time.Since(start))
				startUnmarshalTime = time.Now()
			},
		}
	})

	var payloadSize int
	req.Handlers.Unmarshal.PushBack(func(req *request.Request) {
		payloadSize += int(req.HTTPRequest.ContentLength)
		r.Stats.AddGetRecordsUnmarshalDuration(time.Since(startUnmarshalTime))
		r.LogDebug("Finished GetRecords Unmarshal, took", time.Since(start))
	})

	// Send the GetRecords request
	r.LogDebug("Starting GetRecords Build/Sign request, took", time.Since(start))
	r.Stats.AddGetRecordsCalled(1)
	if err := req.Send(); err != nil {
		r.LogError("Error getting records:", err)
		switch err.(awserr.Error).Code() {
		case kinesis.ErrCodeProvisionedThroughputExceededException:
			r.Stats.AddProvisionedThroughputExceeded(1)
		default:
			r.LogDebug("Received AWS error:", err.Error())
		}
		return 0, 0, err
	}
	r.Stats.AddGetRecordsDuration(time.Since(start))

	// Process Records
	r.LogDebug(fmt.Sprintf("Finished GetRecords request, %d records from shard %s, took %v\n", len(resp.Records), r.shard, time.Since(start)))
	if resp == nil {
		return 0, 0, errs.ErrNilGetRecordsResponse
	}
	delivered := 0
	r.Stats.AddBatchSize(len(resp.Records))
	for _, record := range resp.Records {
		if record != nil {
			// Allow (only) a pipeOfDeath to trigger an instance shutdown of the loop to deliver messages.
			// Otherwise, a normal cancellation will not prevent getRecords from completing the delivery of
			// the current batch of records.
			select {
			case <-ctx.Done():
				r.LogInfo(fmt.Sprintf("getRecords received ctx.Done() while delivering messages, %d delivered, ~%d dropped", delivered, len(resp.Records)-delivered))
				return delivered, payloadSize, ctx.Err()
			default:
				var wg sync.WaitGroup
				wg.Add(1)
				go fn(message.FromRecord(record), &wg)
				wg.Wait()
				delivered++
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
	return delivered, payloadSize, nil
}

// GetRecord calls getRecords and delivers one record into the messages channel.
func (r *KinesisReader) GetRecord(ctx context.Context, fn MessageHandler) (int, int, error) {
	return r.getRecords(ctx, fn, 1)
}

// GetRecords calls getRecords and delivers each record into the messages channel.
func (r *KinesisReader) GetRecords(ctx context.Context, fn MessageHandler) (int, int, error) {
	return r.getRecords(ctx, fn, r.batchSize)
}
