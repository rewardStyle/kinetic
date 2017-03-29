package listener

import (
	"time"
)

// StatsListener allows for a listener to "listen" for various metrics produced
// by the Kinetic library.  This was really built with rcrowley/go-metrics in
// mind.
type StatsListener interface {
	// histograms
	AddConsumedSample(int)
	AddDeliveredSample(int)
	AddProcessedSample(int)
	AddBatchSizeSample(int)

	// meters
	AddGetRecordsCalled(int)
	AddProvisionedThroughputExceeded(int)
	AddGetRecordsTimeout(int)
	AddGetRecordsReadTimeout(int)

	// timers
	AddProcessedTime(time.Duration)
	AddGetRecordsTime(time.Duration)
	AddGetRecordsReadResponseTime(time.Duration)
	AddGetRecordsUnmarshalTime(time.Duration)
}

// NilStatsListener is a stats listener that ignores all metrics.
type NilStatsListener struct{}

// AddConsumedSample records a count of the number of messages received from AWS
// Kinesis by the listener.
func (l *NilStatsListener) AddConsumedSample(int) {}

// AddDeliveredSample records a count of the number of messages delivered to the
// application by the listener.
func (l *NilStatsListener) AddDeliveredSample(int) {}

// AddProcessedSample records a count of the number of messages processed by the
// application by the listener.  This is based on a WaitGroup that is sent to
// the RetrieveFn and Listen functions.  Retrieve does not count processed
// messages.
func (l *NilStatsListener) AddProcessedSample(int) {}

// AddBatchSizeSample records a count of the number of messages returned by
// GetRecords in the listener.
func (l *NilStatsListener) AddBatchSizeSample(int) {}

// AddGetRecordsCalled records the number of times the GetRecords API was called
// by the listener.
func (l *NilStatsListener) AddGetRecordsCalled(int) {}

// AddProvisionedThroughputExceeded records the number of times the GetRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the listener.
func (l *NilStatsListener) AddProvisionedThroughputExceeded(int) {}

// AddGetRecordsTimeout records the number of times the GetRecords API timed out
// on the HTTP level.  This is influenced by the WithHTTPClientTimeout
// configuration.
func (l *NilStatsListener) AddGetRecordsTimeout(int) {}

// AddGetRecordsReadTimeout records the number of times the GetRecords API timed out
// while reading the response body.  This is influenced by the
// WithGetRecordsReadTimeout configuration.
func (l *NilStatsListener) AddGetRecordsReadTimeout(int) {}

// AddProcessedTime records the duration to process a record.  See notes on
// AddProcessedSample.
func (l *NilStatsListener) AddProcessedTime(time.Duration) {}

// AddGetRecordsTime records the duration that the GetRecords API request took.
// Only the times of successful calls are measured.
func (l *NilStatsListener) AddGetRecordsTime(time.Duration) {}

// AddGetRecordsReadResponseTime records the duration that it took to read the
// response body of a GetRecords API request.
func (l *NilStatsListener) AddGetRecordsReadResponseTime(time.Duration) {}

// AddGetRecordsUnmarshalTime records the duration that it took to unmarshal the
// response body of a GetRecords API request.
func (l *NilStatsListener) AddGetRecordsUnmarshalTime(time.Duration) {}
