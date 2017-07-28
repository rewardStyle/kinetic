package kinetic

import (
	"errors"
)

var (
	// ErrNilDescribeStreamResponse is an error returned by GetShards when
	// the DescribeStream request returns a nil response
	ErrNilDescribeStreamResponse = errors.New("DescribeStream returned a nil response")

	// ErrNilStreamDescription is an error returned by GetShards when the
	// DescribeStream request returns a response with a nil
	// StreamDescription
	ErrNilStreamDescription = errors.New("DescribeStream returned a nil StreamDescription")

	// ErrPipeOfDeath returns when the pipe of death is closed.
	ErrPipeOfDeath = errors.New("Received pipe of death")
)

var (
	// ErrNilListener is returned when the Listener is nil when it is not
	// supposed to be
	ErrNilListener = errors.New("StreamReader not associated with a listener")

	// ErrListenerAlreadyAssociated is returned by a StreamReader attempting
	// to associate it with a Listener when it already has an association
	// with a listener
	ErrListenerAlreadyAssociated = errors.New("StreamReader already associated with a listener")

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
	ErrNilGetShardIteratorResponse = errors.New("GetShardIterator returned a nil response")

	// ErrNilShardIterator is returned when the GetShardIterator call
	// returns a nil shard iterator.
	ErrNilShardIterator = errors.New("GetShardIterator returned a nil ShardIterator")

	// ErrNilGetRecordsResponse is returned when the GetRecords calls
	// returns a nil response.
	ErrNilGetRecordsResponse = errors.New("GetRecords returned an nil response")

	// ErrTimeoutReadResponseBody is returned when a timeout occurs while
	// reading the GetRecords response body.
	ErrTimeoutReadResponseBody = errors.New("Timeout while reading response body")
)

var (
	// ErrNilProducer is returned by a StreamWriter when it has not been
	// correctly associated with a Producer.
	ErrNilProducer = errors.New("StreamWriter not associated with a producer")

	// ErrProducerAlreadyAssociated is returned by a StreamWriter attempting
	// to associate it with a Producer when it already has an association
	// with a producer.
	ErrProducerAlreadyAssociated = errors.New("StreamWriter already associated with a producer")

	// ErrNilPutRecordsResponse is returned when the PutRecords call returns
	// a nil response.
	ErrNilPutRecordsResponse = errors.New("PutRecords returned a nil response")

	// ErrNilFailedRecordCount is returned when the PutRecords call returns
	// a nil FailedRecordCount.
	ErrNilFailedRecordCount = errors.New("GetFailedRecordCount returned a nil FailedRecordCount")

	// ErrRetryRecords is returned when the PutRecords calls requires some
	// records of the batch to be retried.  This failure is considered part
	// of normal behavior of the Kinesis stream.
	ErrRetryRecords = errors.New("PutRecords requires retry of some records in batch")

	// ErrDroppedMessage is returned when the message channel is full and messages are being dropped
	ErrDroppedMessage = errors.New("Channel is full, dropped message")

	// ErrInvalidBatchSize is returned when the batchSize is invalid
	ErrInvalidBatchSize = errors.New("PutRecordsBatch supports batch sizes less than or equal to 500")

	// ErrInvalidMsgCountRateLimit is returned when a MsgCountRateLimit is configured incorrectly
	ErrInvalidMsgCountRateLimit = errors.New("Invalid Message Count Rate Limit")

	// ErrInvalidMsgSizeRateLimit is returned when a MsgCountSizesLimit is configured incorrectly
	ErrInvalidMsgSizeRateLimit = errors.New("Invalid Message Size Rate Limit")

	// ErrInvalidThroughputMultiplier is returned when a ThroughputMultiplier is configured incorrectly
	ErrInvalidThroughputMultiplier = errors.New("Invalid Throughput Multiplier")

	// ErrInvalidQueueDepth is returned when the Queue Depth is configured incorrectly
	ErrInvalidQueueDepth = errors.New("Invalid Queue Depth")

	// ErrInvalidMaxRetryAttempts is returned when the Max Retry Attempts is configured incorrectly
	ErrInvalidMaxRetryAttempts = errors.New("Invalid Max Retry Attempts")

	// ErrInvalidConcurrency is returned when the concurrency value is configured incorrectly
	ErrInvalidConcurrency = errors.New("Invalid concurrency")
)
