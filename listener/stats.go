package listener

import (
	"time"
)

// StatsCollector allows for a collector to collect various metrics produced by
// the Kinetic Listener library.  This was really built with rcrowley/go-metrics
// in mind.
type StatsCollector interface {
	AddConsumed(int)
	AddDelivered(int)
	AddProcessed(int)
	AddBatchSize(int)
	AddGetRecordsCalled(int)
	AddProvisionedThroughputExceeded(int)
	AddGetRecordsTimeout(int)
	AddGetRecordsReadTimeout(int)

	AddProcessedDuration(time.Duration)
	AddGetRecordsDuration(time.Duration)
	AddGetRecordsReadResponseDuration(time.Duration)
	AddGetRecordsUnmarshalDuration(time.Duration)
}

// NilStatsCollector is a stats listener that ignores all metrics.
type NilStatsCollector struct{}

// AddConsumed records a count of the number of messages received from AWS
// Kinesis by the listener.
func (l *NilStatsCollector) AddConsumed(int) {}

// AddDelivered records a count of the number of messages delivered to the
// application by the listener.
func (l *NilStatsCollector) AddDelivered(int) {}

// AddProcessed records a count of the number of messages processed by the
// application by the listener.  This is based on a WaitGroup that is sent to
// the RetrieveFn and Listen functions.  Retrieve does not count processed
// messages.
func (l *NilStatsCollector) AddProcessed(int) {}

// AddBatchSize records a count of the number of messages returned by
// GetRecords in the listener.
func (l *NilStatsCollector) AddBatchSize(int) {}

// AddGetRecordsCalled records the number of times the GetRecords API was called
// by the listener.
func (l *NilStatsCollector) AddGetRecordsCalled(int) {}

// AddProvisionedThroughputExceeded records the number of times the GetRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the listener.
func (l *NilStatsCollector) AddProvisionedThroughputExceeded(int) {}

// AddGetRecordsTimeout records the number of times the GetRecords API timed out
// on the HTTP level.  This is influenced by the WithHTTPClientTimeout
// configuration.
func (l *NilStatsCollector) AddGetRecordsTimeout(int) {}

// AddGetRecordsReadTimeout records the number of times the GetRecords API timed
// out while reading the response body.  This is influenced by the
// WithGetRecordsReadTimeout configuration.
func (l *NilStatsCollector) AddGetRecordsReadTimeout(int) {}

// AddProcessedDuration records the duration to process a record.  See notes on
// AddProcessed.
func (l *NilStatsCollector) AddProcessedDuration(time.Duration) {}

// AddGetRecordsDuration records the duration that the GetRecords API request
// took.  Only the times of successful calls are measured.
func (l *NilStatsCollector) AddGetRecordsDuration(time.Duration) {}

// AddGetRecordsReadResponseDuration records the duration that it took to read
// the response body of a GetRecords API request.
func (l *NilStatsCollector) AddGetRecordsReadResponseDuration(time.Duration) {}

// AddGetRecordsUnmarshalDuration records the duration that it took to unmarshal
// the response body of a GetRecords API request.
func (l *NilStatsCollector) AddGetRecordsUnmarshalDuration(time.Duration) {}
