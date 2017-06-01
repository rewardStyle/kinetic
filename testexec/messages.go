package main

import (
	"sync/atomic"
	"strconv"
)

var msgCount uint64

// Message is a data structure for sending / receiving messages over a kinetic stream
type Message struct {
	ID      uint64 `json:"id"`
	Message string `json:"message"`
}

// NewMessage creates a new Message struct with a unique identifier
func NewMessage() *Message {
	atomic.AddUint64(&msgCount, 1)
	id := atomic.LoadUint64(&msgCount)
	return &Message{
		ID: id,
		Message: "hello_" + strconv.Itoa(int(id)),
	}
}
