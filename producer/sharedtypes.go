package producer

import (
	"context"
	"sync"

	"github.com/rewardStyle/kinetic"
)

// StreamWriter is an interface that abstracts the differences in API between Kinesis and Firehose.
type StreamWriter interface {
	PutRecords(context.Context, []*kinetic.Message, MessageHandlerAsync) error
	getMsgCountRateLimit() int
	getMsgSizeRateLimit() int
	getConcurrencyMultiplier() (int, error)
}

// MessageHandler defines the signature of a message handler used by PutRecords().  MessageHandler accepts a WaitGroup
// so the function can be run as a blocking operation as opposed to MessageHandlerAsync.
type MessageHandler func(*kinetic.Message, *sync.WaitGroup) error

// MessageHandlerAsync defines the signature of a message handler used by PutRecords().  MessageHandlerAsync is meant to
// be run asynchronously.
type MessageHandlerAsync func(*kinetic.Message) error

// statusReport is used to communicate a worker's capacity for new messages and to which channel they should be sent.
type statusReport struct {
	capacity    int                     // maximum message capacity the worker can handle
	failedCount int                     // number of previous messages that failed to send
	failedSize  int                     // size in bytes of the previous messages that failed to send
	channel     chan []*kinetic.Message // channel of the worker to which the batch messages should be sent
}

// empty is used a as a dummy type for counting semaphore channels.
type empty struct{}

// noCopy is used to prevent structs from being copied
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}
