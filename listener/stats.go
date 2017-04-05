package listener

import (
	"time"
)

// StatsCollector allows for a collector to collect various metrics produced by
// the Kinetic Listener library.  This was really built with rcrowley/go-metrics
// in mind.
type StatsCollector interface {
	// histograms give us the count, sum, min, max, mean, percentiles,
	// standard deviation, and variance of the data.  these metrics should
	// give us the total number (sum) of messages consumed, delivered, and
	// processed, as well as the average (mean) batch size.
	AddConsumedSample(int)
	AddDeliveredSample(int)
	AddProcessedSample(int)
	AddBatchSizeSample(int)

	// meters give us the count and rate of the data.  these metrics should
	// give us the average number of times:
	//   - GetRecords was called per second
	//   - ProvisionedThroughputExceeded was received per second
	//   - GetRecords timed out per second
	//   - GetRecords read timed out per second
	AddGetRecordsCalled(int)
	AddProvisionedThroughputExceeded(int)
	AddGetRecordsTimeout(int)
	AddGetRecordsReadTimeout(int)

	// timers give us the count, sum, min, max, mean, percentiles, standard
	// deviation, variance, as well as the rate of the data.
	// TODO: describe these metrics better
	AddProcessedTime(time.Duration)
	AddGetRecordsTime(time.Duration)
	AddGetRecordsReadResponseTime(time.Duration)
	AddGetRecordsUnmarshalTime(time.Duration)
}

// NilStatsCollector is a stats listener that ignores all metrics.
type NilStatsCollector struct{}

// AddConsumedSample records a count of the number of messages received from AWS
// Kinesis by the listener.
func (l *NilStatsCollector) AddConsumedSample(int) {}

// AddDeliveredSample records a count of the number of messages delivered to the
// application by the listener.
func (l *NilStatsCollector) AddDeliveredSample(int) {}

// AddProcessedSample records a count of the number of messages processed by the
// application by the listener.  This is based on a WaitGroup that is sent to
// the RetrieveFn and Listen functions.  Retrieve does not count processed
// messages.
func (l *NilStatsCollector) AddProcessedSample(int) {}

// AddBatchSizeSample records a count of the number of messages returned by
// GetRecords in the listener.
func (l *NilStatsCollector) AddBatchSizeSample(int) {}

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

// AddProcessedTime records the duration to process a record.  See notes on
// AddProcessedSample.
func (l *NilStatsCollector) AddProcessedTime(time.Duration) {}

// AddGetRecordsTime records the duration that the GetRecords API request took.
// Only the times of successful calls are measured.
func (l *NilStatsCollector) AddGetRecordsTime(time.Duration) {}

// AddGetRecordsReadResponseTime records the duration that it took to read the
// response body of a GetRecords API request.
func (l *NilStatsCollector) AddGetRecordsReadResponseTime(time.Duration) {}

// AddGetRecordsUnmarshalTime records the duration that it took to unmarshal the
// response body of a GetRecords API request.
func (l *NilStatsCollector) AddGetRecordsUnmarshalTime(time.Duration) {}
