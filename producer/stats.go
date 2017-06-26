package producer

import (
	"time"
)

// StatsCollector allows for a collector to collect various metrics produced by
// the Kinetic producer library.  This was really built with rcrowley/go-metrics
// in mind.
type StatsCollector interface {
	AddSent(int)
	AddFailed(int)
	AddDroppedTotal(int)
	AddDroppedCapacity(int)
	AddDroppedRetries(int)
	AddBatchSize(int)
	AddPutRecordsProvisionedThroughputExceeded(int)
	AddPutRecordsCalled(int)
	AddProvisionedThroughputExceeded(int)
	AddPutRecordsTimeout(int)

	AddPutRecordsDuration(time.Duration)
	AddPutRecordsBuildDuration(time.Duration)
	AddPutRecordsSendDuration(time.Duration)
}

// NilStatsCollector is a stats listener that ignores all metrics.
type NilStatsCollector struct{}

// AddSent records a count of the number of messages sent to AWS Kinesis by the producer.
func (l *NilStatsCollector) AddSent(int) {}

// AddFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (l *NilStatsCollector) AddFailed(int) {}

// AddDroppedTotal records a count of the total number of messages dropped by the application after multiple failures.
func (l *NilStatsCollector) AddDroppedTotal(int) {}

// AddDroppedCapacity records a count of the number of messages that were dropped by the application due to the stream
// writer being at capacity.
func (l *NilStatsCollector) AddDroppedCapacity(int) {}

// AddDroppedRetries records a count of the number of retry messages dropped by the application after the max number of
// retries was exceeded.
func (l *NilStatsCollector) AddDroppedRetries(int) {}

// AddBatchSize records a count of the number of messages attempted by PutRecords in the producer.
func (l *NilStatsCollector) AddBatchSize(int) {}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (l *NilStatsCollector) AddPutRecordsProvisionedThroughputExceeded(int) {}

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (l *NilStatsCollector) AddPutRecordsCalled(int) {}

// AddProvisionedThroughputExceeded records the number of times the PutRecords API response contained a record which
// contained an ErrCodeProvisionedThroughputExceededException error.
func (l *NilStatsCollector) AddProvisionedThroughputExceeded(int) {}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (l *NilStatsCollector) AddPutRecordsTimeout(int) {}

// AddPutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (l *NilStatsCollector) AddPutRecordsDuration(time.Duration) {}

// AddPutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (l *NilStatsCollector) AddPutRecordsBuildDuration(time.Duration) {}

// AddPutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (l *NilStatsCollector) AddPutRecordsSendDuration(time.Duration) {}
