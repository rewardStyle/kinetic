package consumer

import (
	"context"
	"sync"

	"github.com/rewardStyle/kinetic"
)

// StreamReader is an interface that abstracts out a stream reader.
type StreamReader interface {
	GetRecord(context.Context, MessageHandler) (int, int, error)
	GetRecords(context.Context, MessageHandler) (int, int, error)
}

// empty is used a as a dummy type for semaphore channels and the pipe of death channel.
type empty struct{}

// MessageProcessor defines the signature of a message handler used by Listen, RetrieveFn and their associated
// *WithContext functions.  MessageHandler accepts a WaitGroup so the function can be run as a blocking operation as
// opposed to MessageHandlerAsync.
type MessageProcessor func(*kinetic.Message, *sync.WaitGroup) error

// MessageHandler defines the signature of a message handler used by GetRecord() and GetRecords().  MessageHandler
// accepts a WaitGroup so the function can be run as a blocking operation as opposed to MessageHandlerAsync.
type MessageHandler func(*kinetic.Message, *sync.WaitGroup) error

// MessageHandlerAsync defines the signature of a message handler used by GetRecord() and GetRecords().
// MessageHandlerAsync is meant to be run asynchronously.
type MessageHandlerAsync func(*kinetic.Message) error
