package kinetic

import (
	"context"
)

// StreamWriter is an interface that abstracts the differences in API between Kinesis and Firehose.
type StreamWriter interface {
	PutRecords(context.Context, []*Message, messageHandler) error
	getMsgCountRateLimit() int
	getMsgSizeRateLimit() int
	getConcurrencyMultiplier() (int, error)
}

// StreamReader is an interface that abstracts out a stream reader.
type StreamReader interface {
	GetRecord(context.Context, messageHandler) error
	GetRecords(context.Context, messageHandler) error
}

// MessageProcessor defines the signature of a (asynchronous) callback function used by Listen, RetrieveFn and
// their associated *WithContext functions.  MessageProcessor is a user-defined callback function for processing
// messages after they have been pulled off of the consumer's message channel or for processing the producer's
// dropped message.
type MessageProcessor func(*Message) error

// messageHandler defines the signature of a message handler used by PutRecords(), GetRecord() and GetRecords().
// The messageHandler is used as a callback function defined by the producer/consumer so the writers/readers
// don't need to know about the producer's/consumer's message channels.
type messageHandler func(*Message) error

// empty is used a as a dummy type for counting semaphore channels.
type empty struct{}

// noCopy is used to prevent structs from being copied
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}

// statusReport is used to communicate a worker's capacity for new messages and to which channel they should be sent.
type statusReport struct {
	capacity    int             // maximum message capacity the worker can handle
	failedCount int             // number of previous messages that failed to send
	failedSize  int             // size in bytes of the previous messages that failed to send
	channel     chan []*Message // channel of the worker to which the batch messages should be sent
}
