package main

import (
	"math/rand"
	"sync/atomic"
)

var msgCount uint64

// Message is a data structure for sending / receiving messages over a kinetic stream
type Message struct {
	ID      uint64 `json:"id"`
	Message string `json:"message"`
}

// NewMessage creates a new Message struct with a unique identifier
func NewMessage() *Message {
	// Increment ID
	atomic.AddUint64(&msgCount, 1)
	id := atomic.LoadUint64(&msgCount)

	// Create random string as message
	message := make([]byte, 100)
	rand.Read(message)

	return &Message{
		ID:      id,
		Message: string(message),
	}
}
