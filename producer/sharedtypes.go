package producer

import (
	"context"
	"sync"

	"github.com/rewardStyle/kinetic/message"
)

// StreamWriter is an interface that abstracts the differences in API between Kinesis and Firehose.
type StreamWriter interface {
	PutRecords(context.Context, []*message.Message, MessageHandlerAsync) error
}

// MessageHandler defines the signature of a message handler used by PutRecords().  MessageHandler accepts a WaitGroup
// so the function can be run as a blocking operation as opposed to MessageHandlerAsync.
type MessageHandler func(*message.Message, *sync.WaitGroup) error

// MessageHandlerAsync defines the signature of a message handler used by PutRecords().  MessageHandlerAsync is meant to
// be run asynchronously.
type MessageHandlerAsync func(*message.Message) error

// statusReport is used to communicate a worker's capacity for new messages and to which channel they should be sent.
type statusReport struct {
	capacity int                     // maximum message capacity the worker can handle
	failed   int                     // number of previous messages that failed to send
	channel  chan []*message.Message // channel of the worker to which the batch messages should be sent
}

// empty is used a as a dummy type for counting semaphore channels.
type empty struct{}

// noCopy is used to prevent structs from being copied
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}
