package main

import (
	"log"
	"sync"
)

// StreamData is used to collect stream stats
type StreamData struct {
	mutex       sync.Mutex
	Frequencies map[int]int
	Messages    map[int][]string
}

// NewStreamData instantiates a new StreamData struct
func NewStreamData() *StreamData {
	return &StreamData{
		mutex: sync.Mutex{},
		Frequencies: make(map[int]int),
		Messages: make(map[int][]string),
	}
}

func (m *StreamData) put(key int, value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Frequencies[key] = m.Frequencies[key] + 1
	m.Messages[key] = append(m.Messages[key], value)
}

func (m *StreamData) exists(key int) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.Frequencies[key] > 0
}

func (m *StreamData) size() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.Messages)
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

func (m *StreamData) printSummary() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	log.Println("***** Stream Data Summary *****")
	log.Printf("Total messages sent/received: [%d]\n", len(m.Messages))
	for index, freq := range m.Frequencies {
		if freq > 1 {
			log.Printf("Message [%d] was received [%d] times\n", index, freq)
		}
	}
	log.Println("***** Stream Data Summary *****")
}
