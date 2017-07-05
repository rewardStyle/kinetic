package main

import (
	"sync/atomic"
	"time"
	"log"
)

// ProducerStatsCollector implements the producer's StatsCollector
type ProducerStatsCollector struct {
	Sent                                    uint64
	Failed                                  uint64
	DroppedTotal                            uint64
	DroppedCapacity                         uint64
	DroppedRetries                          uint64
	BatchSize                               uint64
	PutRecordsProvisionedThroughputExceeded uint64
	PutRecordsCalled                        uint64
	ProvisionedThroughputExceeded           uint64
	PutRecordsTimeout                       uint64

	PutRecordsDuration                      time.Duration
	PutRecordsBuildDuration                 time.Duration
	PutRecordsSendDuration                  time.Duration
}

// GetSent returns the number of messages sent to AWS Kinesis by the producer.
func (psc *ProducerStatsCollector) GetSent() uint64 {
	return atomic.LoadUint64(&psc.Sent)
}

// AddSent records a count of the number of messages sent to AWS Kinesis by the producer.
func (psc *ProducerStatsCollector) AddSent(count int) {
	atomic.AddUint64(&psc.Sent, uint64(count))
}

// AddFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (psc *ProducerStatsCollector) AddFailed(count int) {
	atomic.AddUint64(&psc.Failed, uint64(count))
}

// AddDroppedTotal records a count of the total number of messages dropped by the application after multiple failures.
func (psc *ProducerStatsCollector) AddDroppedTotal(count int) {
	atomic.AddUint64(&psc.DroppedTotal, uint64(count))
}

// AddDroppedCapacity records a count of the number of messages that were dropped by the application due to the stream
// writer being at capacity.
func (psc *ProducerStatsCollector) AddDroppedCapacity(count int) {
	atomic.AddUint64(&psc.DroppedCapacity, uint64(count))
}

// AddDroppedRetries records a count of the number of retry messages dropped by the application after the max number of
// retries was exceeded.
func (psc *ProducerStatsCollector) AddDroppedRetries(count int) {
	atomic.AddUint64(&psc.DroppedRetries, uint64(count))
}

// AddBatchSize records a count of the number of messages attempted by PutRecords in the producer.
func (psc *ProducerStatsCollector) AddBatchSize(count int) {
	atomic.AddUint64(&psc.BatchSize, uint64(count))
}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (psc *ProducerStatsCollector) AddPutRecordsProvisionedThroughputExceeded(count int) {
	atomic.AddUint64(&psc.PutRecordsProvisionedThroughputExceeded, uint64(count))
}

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (psc *ProducerStatsCollector) AddPutRecordsCalled(count int) {
	atomic.AddUint64(&psc.PutRecordsCalled, uint64(count))
}

// AddProvisionedThroughputExceeded records the number of times the PutRecords API response contained a record which
// contained an ErrCodeProvisionedThroughputExceededException error.
func (psc *ProducerStatsCollector) AddProvisionedThroughputExceeded(count int) {
	atomic.AddUint64(&psc.ProvisionedThroughputExceeded, uint64(count))
}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (psc *ProducerStatsCollector) AddPutRecordsTimeout(count int) {
	atomic.AddUint64(&psc.PutRecordsTimeout, uint64(count))
}

// AddPutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (psc *ProducerStatsCollector) AddPutRecordsDuration(duration time.Duration) {
	// TODO: Not threadsafe
	psc.PutRecordsDuration = duration
}

// AddPutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (psc *ProducerStatsCollector) AddPutRecordsBuildDuration(duration time.Duration) {
	// TODO: Not threadsafe
	psc.PutRecordsBuildDuration = duration
}

// AddPutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (psc *ProducerStatsCollector) AddPutRecordsSendDuration(duration time.Duration) {
	// TODO: Not threadsafe
	psc.PutRecordsSendDuration = duration
}

// PrintStats logs stats atomically
func (psc *ProducerStatsCollector) PrintStats() {
	log.Printf("Producer Stats: Sent: [%d]\n", atomic.LoadUint64(&psc.Sent))
	log.Printf("Producer Stats: Failed: [%d]\n", atomic.LoadUint64(&psc.Failed))
	log.Printf("Producer Stats: Dropped Total: [%d]\n", atomic.LoadUint64(&psc.DroppedTotal))
	log.Printf("Producer Stats: Dropped Retries: [%d]\n", atomic.LoadUint64(&psc.DroppedRetries))
	log.Printf("Producer Stats: Dropped Capacity: [%d]\n", atomic.LoadUint64(&psc.DroppedCapacity))
	log.Printf("Producer Stats: Batch Size: [%d]\n", atomic.LoadUint64(&psc.BatchSize))
	log.Printf("Producer Stats: PutRecords Called: [%d]\n", atomic.LoadUint64(&psc.PutRecordsCalled))
	log.Printf("Producer Stats: Provisioned Throughput Exceeded: [%d]\n", atomic.LoadUint64(&psc.ProvisionedThroughputExceeded))
	log.Printf("Producer Stats: PutRecords Timeout: [%d]\n", atomic.LoadUint64(&psc.PutRecordsTimeout))
}
