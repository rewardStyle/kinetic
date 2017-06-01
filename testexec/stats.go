package main

import (
	"log"
	"sync"
	"sync/atomic"
)

// StreamData is used to collect stream stats
type StreamData struct {
	mutex       sync.Mutex
	MsgCount    uint64
	Frequencies map[uint64]uint64
	Duplicates  uint64
}

// NewStreamData instantiates a new StreamData struct
func NewStreamData() *StreamData {
	return &StreamData{
		mutex: sync.Mutex{},
		Frequencies: make(map[uint64]uint64),
	}
}

func (m *StreamData) incrementMsgCount() {
	atomic.AddUint64(&m.MsgCount, 1)
}

func (m *StreamData) mark(key uint64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Frequencies[key] = m.Frequencies[key] + 1
	if m.Frequencies[key] > 1 {
		m.Duplicates++
	}
}

func (m *StreamData) exists(key uint64) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.Frequencies[key]

	return ok
}

func (m *StreamData) size() uint64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return uint64(len(m.Frequencies))
}

func (m *StreamData) hasDuplicates() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, freq := range m.Frequencies {
		if freq > 1 {
			return true
		}
	}
	return false
}

func (m *StreamData) printStats() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	log.Println()
	log.Println("***** Stream Data Stats *****")
	log.Printf("Messages sent: [%d]\n", m.MsgCount)
	log.Printf("Messages received: [%d]\n", len(m.Frequencies))
	log.Printf("Number of duplicated messages: [%d]\n", m.Duplicates)
}

func (m *StreamData) printSummary() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	log.Println()
	log.Println("***** Stream Data Summary *****")
	log.Printf("Total messages sent: [%d]\n", m.MsgCount)
	log.Printf("Total messages received: [%d]\n", len(m.Frequencies))
	log.Printf("Total Number of duplicated messages: [%d]\n", m.Duplicates)
	for index, freq := range m.Frequencies {
		if freq > 1 {
			log.Printf("Message [%d] was received [%d] times\n", index, freq)
		}
	}
}
