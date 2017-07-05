package main

import (
	"sync/atomic"
	"time"
	"log"
)

// ListenerStatsCollector implements the listener's StatsCollector
type ListenerStatsCollector struct {
	Consumed                      uint64
	Delivered                     uint64
	Processed                     uint64
	BatchSize                     uint64
	GetRecordsCalled              uint64
	ProvisionedThroughputExceeded uint64
	GetRecordsTimeout             uint64
	GetRecordsReadTimeout         uint64

	ProcessedDuration	      time.Duration
	GetRecordsDuration            time.Duration
	GetRecordsReadResonseDuration time.Duration
	GetRecordsUnmarshalDuration   time.Duration
}

// AddConsumed records a count of the number of messages received from AWS
// Kinesis by the listener.
func (lsc *ListenerStatsCollector) AddConsumed(count int) {
	atomic.AddUint64(&lsc.Consumed, uint64(count))
}

// AddDelivered records a count of the number of messages delivered to the
// application by the listener.
func (lsc *ListenerStatsCollector) AddDelivered(count int) {
	atomic.AddUint64(&lsc.Delivered, uint64(count))
}

// AddProcessed records a count of the number of messages processed by the
// application by the listener.  This is based on a WaitGroup that is sent to
// the RetrieveFn and Listen functions.  Retrieve does not count processed
// messages.
func (lsc *ListenerStatsCollector) AddProcessed(count int) {
	atomic.AddUint64(&lsc.Processed, uint64(count))
}

// AddBatchSize records a count of the number of messages returned by
// GetRecords in the listener.
func (lsc *ListenerStatsCollector) AddBatchSize(count int) {
	atomic.AddUint64(&lsc.BatchSize, uint64(count))
}

// AddGetRecordsCalled records the number of times the GetRecords API was called
// by the listener.
func (lsc *ListenerStatsCollector) AddGetRecordsCalled(count int) {
	atomic.AddUint64(&lsc.GetRecordsCalled, uint64(count))
}

// AddProvisionedThroughputExceeded records the number of times the GetRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the listener.
func (lsc *ListenerStatsCollector) AddProvisionedThroughputExceeded(count int) {
	atomic.AddUint64(&lsc.ProvisionedThroughputExceeded, uint64(count))
}

// AddGetRecordsTimeout records the number of times the GetRecords API timed out
// on the HTTP level.  This is influenced by the WithHTTPClientTimeout
// configuration.
func (lsc *ListenerStatsCollector) AddGetRecordsTimeout(count int) {
	atomic.AddUint64(&lsc.GetRecordsTimeout, uint64(count))
}

// AddGetRecordsReadTimeout records the number of times the GetRecords API timed
// out while reading the response body.  This is influenced by the
// WithGetRecordsReadTimeout configuration.
func (lsc *ListenerStatsCollector) AddGetRecordsReadTimeout(count int) {
	atomic.AddUint64(&lsc.GetRecordsReadTimeout, uint64(count))
}

// AddProcessedDuration records the duration to process a record.  See notes on
// AddProcessed.
func (lsc *ListenerStatsCollector) AddProcessedDuration(duration time.Duration) {
	// TODO: Not threadsafe
	lsc.ProcessedDuration = duration
}

// AddGetRecordsDuration records the duration that the GetRecords API request
// took.  Only the times of successful calls are measured.
func (lsc *ListenerStatsCollector) AddGetRecordsDuration(duration time.Duration) {
	// TODO: Not threadsafe
	lsc.GetRecordsDuration = duration

}

// AddGetRecordsReadResponseDuration records the duration that it took to read
// the response body of a GetRecords API request.
func (lsc *ListenerStatsCollector) AddGetRecordsReadResponseDuration(duration time.Duration) {
	// TODO: Not threadsafe
	lsc.GetRecordsReadResonseDuration = duration
}

// AddGetRecordsUnmarshalDuration records the duration that it took to unmarshal
// the response body of a GetRecords API request.
func (lsc *ListenerStatsCollector) AddGetRecordsUnmarshalDuration(duration time.Duration) {
	// TODO: Not threadsafe
	lsc.GetRecordsUnmarshalDuration = duration
}

// PrintStats logs stats atomically
func (lsc *ListenerStatsCollector) PrintStats() {
	log.Printf("Listener stats: Consumed: [%d]\n", atomic.LoadUint64(&lsc.Consumed))
	log.Printf("Listener stats: Delivered: [%d]\n", atomic.LoadUint64(&lsc.Delivered))
	log.Printf("Listener stats: Processed: [%d]\n", atomic.LoadUint64(&lsc.Processed))
	log.Printf("Listener stats: Batch Size: [%d]\n", atomic.LoadUint64(&lsc.BatchSize))
	log.Printf("Listener stats: GetRecords Called: [%d]\n", atomic.LoadUint64(&lsc.GetRecordsCalled))
	log.Printf("Listener stats: Provisioned Throughput Exceeded: [%d]\n", atomic.LoadUint64(&lsc.ProvisionedThroughputExceeded))
	log.Printf("Listener stats: GetRecords Timeout: [%d]\n", atomic.LoadUint64(&lsc.GetRecordsTimeout))
	log.Printf("Listener stats: GetRecords Read Timeout: [%d]\n", atomic.LoadUint64(&lsc.GetRecordsReadTimeout))
}