package producer

import (
	"context"
	"sync"

	"github.com/rewardStyle/kinetic/message"
)

// StreamWriter is an interface that abstracts the differences in API between Kinesis and Firehose.
type StreamWriter interface {
	PutRecords(context.Context, []*message.Message, MessageHandler) error
}

// sendBatchFn defines the signature for the sendBatch function defined by the producer and passed to the worker as a
// closure for execution
type sendBatchFn func([]*message.Message) []*message.Message

// reportStatusFn defines the signature for the worker to communicate its status to the producer
type reportStatusFn func(*statusReport)

// MessageHandler defines the signature of a message handler used by PutRecords().  MessageHandler accepts a WaitGroup
// so the function can be run as a blocking operation as opposed to MessageHandlerAsync.
type MessageHandler func(*message.Message, *sync.WaitGroup) error

// MessageHandlerAsync defines the signature of a message handler used by PutRecords().  MessageHandlerAsync is meant to
// be run asynchronously.
type MessageHandlerAsync func(*message.Message) error

// empty is used a as a dummy type for counting semaphore channels.
type empty struct{}

// noCopy is used to prevent structs from being copied
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}
