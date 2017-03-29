package listener

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/utils"
)

var (
	// ErrAlreadyConsuming is returned when attempting to consume when the
	// Listener is already consuming.  May be returned by
	// Retrieve/RetrieveFn.
	ErrAlreadyConsuming = errors.New("Listener already consuming.  Only one Listen, Retrieve, or RetrieveFn may be active at a time")

	// ErrEmptySequenceNumber is returned when attempting to set an empty
	// sequence number.
	ErrEmptySequenceNumber = errors.New("Attempted to set sequence number with empty value")

	// ErrEmptyShardIterator is returned when attempting to set an empty
	// sequence number.
	ErrEmptyShardIterator = errors.New("Attempted to set shard iterator with empty value")

	// ErrNilGetShardIteratorResponse is returned when the GetShardIterator
	// call returns a nil response.
	ErrNilGetShardIteratorResponse = errors.New("GetShardIteratore returned a nil response")

	// ErrNilShardIterator is returned when the GetShardIterator call
	// returns a nil shard iterator.
	ErrNilShardIterator = errors.New("GetShardIterator returned a nil ShardIterator")

	// ErrNilGetRecordsResponse is returned when the GetRecords calls
	// returns a nil response.
	ErrNilGetRecordsResponse = errors.New("GetRecords returned an nil response")

	// ErrTimeoutReadResponseBody is returned when a timeout occurs while
	// reading the GetRecords response body.
	ErrTimeoutReadResponseBody = errors.New("Timeout while reading response body")

	// ErrPipeOfDeath returns when the pipe of death is closed.
	ErrPipeOfDeath = errors.New("Received pipe of death")
)

// Empty is used a as a dummy type for counting semaphore channels.
type Empty struct{}

// MessageFn defines the signature of a message handler used by Listen and
// RetrieveFn.
type MessageFn func([]byte, *sync.WaitGroup)

// ShardIterator represents the settings used to retrieve a shard iterator from
// the GetShardIterator API.
type ShardIterator struct {
	shardIteratorType string
	sequenceNumber    string
	timestamp         time.Time
}

// NewShardIterator creates a new ShardIterator.  The default shard iterator
// type is TRIM_HORIZON.
func NewShardIterator() *ShardIterator {
	return &ShardIterator{
		shardIteratorType: "TRIM_HORIZON",
	}
}

// TrimHorizon sets the shard iterator to TRIM_HORIZON.
func (it *ShardIterator) TrimHorizon() *ShardIterator {
	it.shardIteratorType = "TRIM_HORIZON"
	return it
}

// Latest sets the shard iterator to LATEST.
func (it *ShardIterator) Latest() *ShardIterator {
	it.shardIteratorType = "LATEST"
	return it
}

// AtSequenceNumber sets the shard iterator to AT_SEQUENCE_NUMBER.
func (it *ShardIterator) AtSequenceNumber(sequenceNumber string) *ShardIterator {
	it.shardIteratorType = "AT_SEQUENCE_NUMBER"
	it.sequenceNumber = sequenceNumber
	return it
}

// AfterSequenceNumber sets the shard iterator to AFTER_SEQUENCE_NUMBER.
func (it *ShardIterator) AfterSequenceNumber(sequenceNumber string) *ShardIterator {
	it.shardIteratorType = "AFTER_SEQUENCE_NUMBER"
	it.sequenceNumber = sequenceNumber
	return it
}

// AtTimestamp sets the shard iterator to AT_TIMESTAMP.
func (it *ShardIterator) AtTimestamp(timestamp time.Time) *ShardIterator {
	it.shardIteratorType = "AT_TIMESTAMP"
	it.timestamp = timestamp
	return it
}

func (it *ShardIterator) getStartingSequenceNumber() *string {
	if it.sequenceNumber == "" {
		return nil
	}
	return aws.String(it.sequenceNumber)
}

func (it *ShardIterator) getTimestamp() *time.Time {
	if it.timestamp.IsZero() {
		return nil
	}
	return aws.Time(it.timestamp)
}

type listenerConfig struct {
	stream string
	shard  string

	batchSize             int
	concurrency           int
	shardIterator         *ShardIterator
	getRecordsReadTimeout time.Duration
	stats                 StatsListener

	logLevel aws.LogLevelType
}

// Listener polls the Kinesis stream for messages.
type Listener struct {
	*listenerConfig

	nextShardIterator string

	messages       chan *message.Message
	concurrencySem chan Empty
	throttleSem    chan Empty
	pipeOfDeath    chan Empty

	consuming   bool
	consumingMu sync.Mutex

	session  *session.Session
	client   kinesisiface.KinesisAPI
	clientMu sync.Mutex
}

// NewListener creates a new listener for listening to message on a Kinesis
// stream.
func NewListener(config *Config) (*Listener, error) {
	session, err := config.GetSession()
	if err != nil {
		return nil, err
	}
	return &Listener{
		listenerConfig: config.listenerConfig,
		concurrencySem: make(chan Empty, config.concurrency),
		throttleSem:    make(chan Empty, 5),
		pipeOfDeath:    make(chan Empty),
		session:        session,
	}, nil
}

// Log a debug message using the AWS SDK logger.
func (l *Listener) Log(args ...interface{}) {
	if l.session.Config.LogLevel.Matches(logging.LogDebug) {
		l.session.Config.Logger.Log(args...)
	}
}

// setNextShardIterator sets the nextShardIterator to use when calling
// GetRecords.
//
// Not thread-safe.  Only called from fetchBatch (and ensureShardIterator,
// which is called from fetchBatch).  Care must be taken to ensure that only
// one call to Listen and Retrieve/RetrieveFn can be running at a time.
func (l *Listener) setNextShardIterator(shardIterator string) error {
	if len(shardIterator) == 0 {
		return ErrEmptyShardIterator
	}
	l.nextShardIterator = shardIterator
	return nil
}

// setSequenceNumber sets the sequenceNumber of shardIterator to the last
// delivered message and updates the shardIteratorType to AT_SEQUENCE_NUMBER.
// This is only used when we need to call getShardIterator (say, to refresh the
// shard iterator).
//
// Not thread-safe.  Only called from fetchBatch.  Care must be taken to ensure
// that only one call to Listen and Retrieve/RetrieveFn can be running at a
// time.
func (l *Listener) setSequenceNumber(sequenceNumber string) error {
	if len(sequenceNumber) == 0 {
		return ErrEmptySequenceNumber
	}
	l.shardIterator.AtSequenceNumber(sequenceNumber)
	return nil
}

// ensureClient will lazily make sure we have an AWS Kinesis client.
func (l *Listener) ensureClient() {
	// From the aws-go-sdk documentation:
	// http://docs.aws.amazon.com/sdk-for-go/api/aws/session/
	//
	// Concurrency:
	// Sessions are safe to use concurrently as long as the Session is not
	// being modified.  The SDK will not modify the Session once the Session
	// has been created.  Creating service clients concurrently from a
	// shared Session is safe.
	//
	// We need to think through the impact of creating a new client (for
	// example, after receiving an error from Kinesis) while there may be
	// outstanding goroutines still processing messages.  My cursory thought
	// is that this is safe to do, as any outstanding messages will likely
	// not interact with the Kinesis stream.  At worst, we would need a lock
	// around the ensureClient method to make sure that no two goroutines
	// are trying to ensure the client at the same time.
	//
	// As we don't expose any methods (or in fact, even the Listener object
	// itself) to the client through the API, I don't forsee needing to add
	// this lock unless something dramatically changes about the design of
	// this library.
	l.clientMu.Lock()
	defer l.clientMu.Unlock()
	if l.client == nil {
		l.client = kinesis.New(l.session)
	}
}

// ensureShardIterator will lazily make sure that we have a valid ShardIterator,
// calling the GetShardIterator API with the configured ShardIteratorType (with
// any applicable StartingSequenceNumber or Timestamp) if necessary.
//
// Not thread-safe.  Only called from fetchBatch Care must be taken to ensure
// that only one call to Listen and Retrieve/RetrieveFn can be running at a
// time.
func (l *Listener) ensureShardIterator() error {
	l.ensureClient()
	if l.nextShardIterator != "" {
		return nil
	}

	resp, err := l.client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:                aws.String(l.shard),                           // Required
		ShardIteratorType:      aws.String(l.shardIterator.shardIteratorType), // Required
		StreamName:             aws.String(l.stream),                          // Required
		StartingSequenceNumber: l.shardIterator.getStartingSequenceNumber(),
		Timestamp:              l.shardIterator.getTimestamp(),
	})
	if err != nil {
		l.Log(err)
		return err
	}
	if resp == nil {
		return ErrNilGetShardIteratorResponse
	}
	if resp.ShardIterator == nil {
		return ErrNilShardIterator
	}
	return l.setNextShardIterator(*resp.ShardIterator)
}

// Kinesis allows five read ops per second per shard.
// http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html
func (l *Listener) throttle(sem chan Empty) {
	sem <- Empty{}
	time.AfterFunc(1*time.Second, func() {
		<-sem
	})
}

// fetchBatch calls GetRecords and delivers each record into the messages
// channel.
// TODO: Convert timeout implementation to use context.Context
// FIXME: Need to investigate that the timeout implementation doesn't result in
// an fd leak.  Since we call Read on the HTTPResonse.Body in a select with a
// timeout channel, we do prevent ourself from blocking.  Once we timeout, we
// return an error to the outer ioutil.ReadAll, which should result in a call
// to our io.ReadCloser's Close function.  This will in turn call Close on the
// underlying HTTPResponse.Body.  The question is whether this actually shuts
// down the TCP connection.  Worst case scenario is that our client Timeout
// eventually fires and closes the socket, but this can be susceptible to FD
// exhaustion.
func (l *Listener) fetchBatch(size int) (int, error) {
	l.ensureClient()
	if err := l.ensureShardIterator(); err != nil {
		return 0, err
	}

	l.throttle(l.throttleSem)

	// We use the GetRecordsRequest method of creating requests to allow for
	// registering custom handlers for better control over the API request.
	var startReadTime time.Time
	var startUnmarshalTime time.Time
	start := time.Now()
	req, resp := l.client.GetRecordsRequest(&kinesis.GetRecordsInput{
		Limit:         aws.Int64(int64(size)),
		ShardIterator: aws.String(l.nextShardIterator),
	})

	// If debug is turned on, add some handlers for GetRecords logging
	if l.session.Config.LogLevel.AtLeast(aws.LogDebug) {
		req.Handlers.Send.PushBack(func(r *request.Request) {
			l.Log("Finished GetRecords Send, took", time.Since(start))
		})
	}

	// Here, we insert a handler to be called after the Send handler and
	// before the the Unmarshal handler in the aws-go-sdk library.
	//
	// The Send handler will call http.Client.Do() on the request, which
	// blocks until the response headers have been read before returning an
	// HTTPResponse.
	//
	// The Unmarshal handler will ultimately call ioutil.ReadAll() on the
	// HTTPResponse.Body stream.
	//
	// Our handler wraps the HTTPResponse.Body with our own ReadCloser so
	// that we can implement a timeout mechanism on the Read() call (which
	// is called by the ioutil.ReadAll() function)
	req.Handlers.Unmarshal.PushFront(func(r *request.Request) {
		l.Log("Started GetRecords Unmarshal, took", time.Since(start))
		// Here, we set a timer that the initial Read() call on
		// HTTPResponse.Body must return by.  Note that the normal
		// http.Client Timeout is still in effect.
		startReadTime = time.Now()
		timer := time.NewTimer(l.getRecordsReadTimeout)

		r.HTTPResponse.Body = &utils.ReadCloserWrapper{
			ReadCloser: r.HTTPResponse.Body,
			OnReadFn: func(stream io.ReadCloser, b []byte) (n int, err error) {
				// The OnReadFn will be called each time
				// ioutil.ReadAll calls Read on the
				// ReadCloserWrapper.

				// First, we set up a struct that to hold the
				// results of the Read() call that can go
				// through a channel
				type Result struct {
					n   int
					err error
				}

				// Next, we build a channel with which to pass
				// the Read() results
				c := make(chan Result, 1)

				// Now, we call the Read() on the
				// HTTPResponse.Body in a goroutine and feed the
				// results into the channel
				readStart := time.Now()
				go func() {
					var result Result
					result.n, result.err = stream.Read(b)
					c <- result
				}()

				// Finally, we poll for the Read() to complete
				// or the timer to elapse.
				select {
				case result := <-c:
					// If we sucessfully Read() from the
					// HTTPResponse.Body, we reset our
					// timeout and return the results from
					// the Read()
					timer.Reset(l.getRecordsReadTimeout)
					n, err = result.n, result.err
					l.Log(fmt.Sprintf("DEBUG: read %d bytes, took %v", n, time.Since(readStart)))
				case <-timer.C:
					// If we timeout, we return an error
					// that will unblock ioutil.ReadAll().
					// This will cause the Unmarshal handler
					// to return an error.  This error will
					// propogate to the original req.Send()
					// call (below)
					l.Log(fmt.Sprintf("DEBUG: read timed out after %v", time.Since(readStart)))
					err = ErrTimeoutReadResponseBody
				}
				return
			},
			OnCloseFn: func() {
				l.stats.AddGetRecordsReadResponseTime(time.Since(startReadTime))
				l.Log("Finished GetRecords body read, took", time.Since(start))
				startUnmarshalTime = time.Now()
			},
		}
	})

	req.Handlers.Unmarshal.PushBack(func(r *request.Request) {
		l.stats.AddGetRecordsUnmarshalTime(time.Since(startUnmarshalTime))
		l.Log("Finished GetRecords Unmarshal, took", time.Since(start))
	})

	// Send the GetRecords request
	l.Log("Starting GetRecords build/sign request, took", time.Since(start))
	l.stats.AddGetRecordsCalled(1)
	if err := req.Send(); err != nil {
		l.Log("Error getting records:", err)
		return 0, err
	}
	l.stats.AddGetRecordsTime(time.Since(start))

	// Process Records
	l.Log(fmt.Sprintf("Finished GetRecords request, %d records from shard %s, took %v\n", len(resp.Records), l.shard, time.Since(start)))
	if resp == nil {
		return 0, ErrNilGetRecordsResponse
	}
	delivered := 0
	l.stats.AddBatchSizeSample(len(resp.Records))
	for _, record := range resp.Records {
		if record != nil {
			delivered++
			l.messages <- &message.Message{Record: *record}
			l.stats.AddConsumedSample(1)
		}
		if record.SequenceNumber != nil {
			// We can safely ignore if this call returns
			// error, as if we somehow receive an empty
			// sequence number from AWS, we will simply not
			// set it.  At worst, this causes us to
			// reprocess this record if we happen to refresh
			// the iterator.
			l.setSequenceNumber(*record.SequenceNumber)
		}
	}
	if resp.NextShardIterator != nil {
		// TODO: According to AWS docs:
		// http://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#GetRecordsOutput
		//
		// NextShardIterator: The next position in the shard
		// from which to start sequentially reading data
		// records.  If set to null, the shard has been closed
		// and the requested iterator will not return any more
		// data.
		//
		// When dealing with streams that will merge or split,
		// we need to detect that the shard has closed and
		// notify the client library.
		//
		// TODO: I don't know if we should be ignoring an error returned
		// by setShardIterator in case of an empty shard iterator in the
		// response.  There isn't much we can do, and the best path for
		// recovery may be simply to reprocess the batch and see if we
		// get a valid NextShardIterator from AWS the next time around.
		l.setNextShardIterator(*resp.NextShardIterator)
	}
	return delivered, nil
}

// blockConsumers will set consuming to true if there is not already another
// consume loop running.
func (l *Listener) blockConsumers() bool {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	if !l.consuming {
		l.consuming = true
		return true
	}
	return false
}

// startConsuming handles any initialization needed in preparation to start
// consuming.
func (l *Listener) startConsuming() {
	l.messages = make(chan *message.Message, l.batchSize)
}

// shouldConsume is a convenience function that allows functions to break their
// loops if the context receives a cancellation.
func (l *Listener) shouldConsume(ctx context.Context) (bool, error) {
	select {
	case <-l.pipeOfDeath:
		return false, ErrPipeOfDeath
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		return true, nil
	}
}

// stopConsuming handles any cleanup after a consuming has stopped.
func (l *Listener) stopConsuming() {
	close(l.messages)
}

// allowConsumers allows consuming.  Called after blockConsumers to release the
// lock on consuming.
func (l *Listener) allowConsumers() {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	l.consuming = false
}

// IsConsuming returns true while consuming.
func (l *Listener) IsConsuming() bool {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	return l.consuming
}

// RetrieveWithContext waits for a message from the stream and return the value.
// Cancellation supported through contexts.
func (l *Listener) RetrieveWithContext(ctx context.Context) (*message.Message, error) {
	if !l.blockConsumers() {
		return nil, ErrAlreadyConsuming
	}
	l.startConsuming()
	defer func() {
		l.stopConsuming()
		l.allowConsumers()
	}()
	for {
		ok, err := l.shouldConsume(ctx)
		if !ok {
			return nil, err
		}
		n, err := l.fetchBatch(1)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			l.stats.AddDeliveredSample(1)
			return <-l.messages, nil
		}
	}
}

// Retrieve waits for a message from the stream and return the value.
func (l *Listener) Retrieve() (*message.Message, error) {
	return l.RetrieveWithContext(context.TODO())
}

// RetrieveFnWithContext retrieves a message from the stream and dispatches it
// to the supplied function.  RetrieveFn will wait until the function completes.
// Cancellation supported through context.
func (l *Listener) RetrieveFnWithContext(ctx context.Context, fn MessageFn) error {
	msg, err := l.RetrieveWithContext(ctx)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	start := time.Now()
	go fn(msg.Value(), &wg)
	wg.Wait()
	l.stats.AddProcessedTime(time.Since(start))
	l.stats.AddProcessedSample(1)
	return nil
}

// RetrieveFn retrieves a message from the stream and dispatches it to the
// supplied function.  RetrieveFn will wait until the function completes.
func (l *Listener) RetrieveFn(fn MessageFn) error {
	return l.RetrieveFnWithContext(context.TODO(), fn)
}

// consume calls fetchBatch with configured batch size in a loop until the
// listener is stopped.
func (l *Listener) consume(ctx context.Context) {
	// We need to run blockConsumers & startConsuming to make sure that we
	// are okay and ready to start consuming.  This is mainly to avoid a
	// race condition where Listen() will attempt to read the messages
	// channel prior to consume() initializing it.  We can then launch a
	// goroutine to handle the actual consume operation.
	if !l.blockConsumers() {
		return
	}
	l.startConsuming()
	go func() {
		defer func() {
			l.stopConsuming()
			l.allowConsumers()
		}()
	stop:
		for {
			ok, err := l.shouldConsume(ctx)
			if !ok {
				break stop
			}
			_, err = l.fetchBatch(l.batchSize)

			if err != nil {
				switch err := err.(type) {
				case net.Error:
					if err.Timeout() {
						l.stats.AddGetRecordsTimeout(1)
						l.Log("Received net error:", err.Error())
					} else {
						l.Log("Received unknown net error:", err.Error())
					}
				case error:
					switch err {
					case ErrTimeoutReadResponseBody:
						l.stats.AddGetRecordsReadTimeout(1)
						l.Log("Received error:", err.Error())
					case ErrEmptySequenceNumber:
						fallthrough
					case ErrEmptyShardIterator:
						fallthrough
					case ErrNilGetShardIteratorResponse:
						fallthrough
					case ErrNilShardIterator:
						fallthrough
					case ErrNilGetRecordsResponse:
						fallthrough
					default:
						l.Log("Received error:", err.Error())
					}
				case awserr.Error:
					switch err.Code() {
					case kinesis.ErrCodeProvisionedThroughputExceededException:
						l.stats.AddProvisionedThroughputExceeded(1)
					case kinesis.ErrCodeResourceNotFoundException:
						fallthrough
					case kinesis.ErrCodeInvalidArgumentException:
						fallthrough
					case kinesis.ErrCodeExpiredIteratorException:
						fallthrough
					default:
						l.Log("Received AWS error:", err.Error())
					}
				default:
					l.Log("Received unknown error:", err.Error())
				}
			}
		}
	}()
}

// ListenWithContext listens and delivers message to the supplied function.
// Upon cancellation, Listen will stop the consumer loop and wait until the
// messages channel is closed and all messages are delivered.
func (l *Listener) ListenWithContext(ctx context.Context, fn MessageFn) {
	l.consume(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()

	for msg := range l.messages {
		l.stats.AddDeliveredSample(1)
		l.concurrencySem <- Empty{}
		wg.Add(1)
		go func(msg *message.Message) {
			defer func() {
				<-l.concurrencySem
			}()
			var fnWg sync.WaitGroup
			fnWg.Add(1)
			start := time.Now()
			fn(msg.Value(), &fnWg)
			fnWg.Wait()
			l.stats.AddProcessedTime(time.Since(start))
			l.stats.AddProcessedSample(1)
			wg.Done()
		}(msg)
	}
}

// Listen listens and delivers message to the supplied function.
func (l *Listener) Listen(fn MessageFn) {
	l.ListenWithContext(context.TODO(), fn)
}
