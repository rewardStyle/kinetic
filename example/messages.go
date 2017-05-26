package main

import (
	"sync/atomic"
	"strconv"
)

var msgCount uint64 = 0

type Message struct {
	Id      int    `json:"id"`
	Message string `json:"message"`
}

func NewMessage() *Message {
	atomic.AddUint64(&msgCount, 1)
	id := int(atomic.LoadUint64(&msgCount))
	return &Message{
		Id: id,
		Message: "hello_" + strconv.Itoa(id),
	}
}
