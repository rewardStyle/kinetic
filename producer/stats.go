package producer

import (
	"time"
)

// StatsCollector allows for a collector to collect various metrics produced by
// the Kinetic producer library.  This was really built with rcrowley/go-metrics
// in mind.
type StatsCollector interface {
	// histograms give us the count, sum, min, max, mean, percentiles,
	// standard deviation, and variance of the data.  these metrics should
	// give us the total number (sum) of messages sent, failed, and dropped,
	// as well as the average (mean) batch size.

	// for producer
	AddSentSample(int)
	AddFailedSample(int)
	AddDroppedSample(int)
	AddBatchSizeSample(int)

	// meters give us the count and rate of the data.  these metrics should
	// give us the average number of times:
	//   - ProvisionedThroughputExceeded was received per second
	//   - PutRecords was called per second
	//   - PutRecordProvisionedThroughputExceeded was received per second
	//   - PutRecords timed out per second

	// for producer
	AddPutRecordsProvisionedThroughputExceeded(int)

	// for kinesiswriter
	AddPutRecordsCalled(int)
	AddProvisionedThroughputExceeded(int)
	AddPutRecordsTimeout(int)

	// timers give us the count, sum, min, max, mean, percentiles, standard
	// deviation, variance, as well as the rate of the data.
	// TODO: describe these metrics better

	// for kinesis writer
	AddPutRecordsTime(time.Duration)
	AddPutRecordsBuildTime(time.Duration)
	AddPutRecordsSendTime(time.Duration)
}

// NilStatsCollector is a stats listener that ignores all metrics.
type NilStatsCollector struct{}

// AddSentSample records a count of the number of messages sent to AWS Kinesis
// by the producer.
func (l *NilStatsCollector) AddSentSample(int) {}

// AddFailedSample records a count of the number of messages that failed to be
// sent to AWS Kinesis by the producer.
func (l *NilStatsCollector) AddFailedSample(int) {}

// AddDroppedSample records a count of the number of messages dropped by the
// application after multiple failures.
func (l *NilStatsCollector) AddDroppedSample(int) {}

// AddBatchSizeSample records a count of the number of messages attempted by
// PutRecords in the producer.
func (l *NilStatsCollector) AddBatchSizeSample(int) {}

// AddProvisionedThroughputExceeded records the number of times the PutRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the producer.
func (l *NilStatsCollector) AddPutRecordsProvisionedThroughputExceeded(int) {}

// AddPutRecordsCalled records the number of times the PutRecords API was called
// by the producer.
func (l *NilStatsCollector) AddPutRecordsCalled(int) {}

// AddProvisionedThroughputExceeded records the number of times the PutRecords
// API response contained a record which contained an
// ErrCodeProvisionedThroughputExceededException error.
func (l *NilStatsCollector) AddProvisionedThroughputExceeded(int) {}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out
// on the HTTP level.  This is influenced by the WithHTTPClientTimeout
// configuration.
func (l *NilStatsCollector) AddPutRecordsTimeout(int) {}

// AddPutRecordsTime records the duration that the PutRecords API request took.
// Only the times of successful calls are measured.
func (l *NilStatsCollector) AddPutRecordsTime(time.Duration) {}

// AddPutRecordsBuildTime records the duration that it took to build the
// PutRecords API request payload.
func (l *NilStatsCollector) AddPutRecordsBuildTime(time.Duration) {}

// AddPutRecordsSendTime records the duration that it took to send the
// PutRecords API request payload.
func (l *NilStatsCollector) AddPutRecordsSendTime(time.Duration) {}
