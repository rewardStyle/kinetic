package producer

import (
	"log"
	"time"

	"github.com/rcrowley/go-metrics"
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
func (nsc *NilStatsCollector) AddSent(int) {}

// AddFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (nsc *NilStatsCollector) AddFailed(int) {}

// AddDroppedTotal records a count of the total number of messages dropped by the application after multiple failures.
func (nsc *NilStatsCollector) AddDroppedTotal(int) {}

// AddDroppedCapacity records a count of the number of messages that were dropped by the application due to the stream
// writer being at capacity.
func (nsc *NilStatsCollector) AddDroppedCapacity(int) {}

// AddDroppedRetries records a count of the number of retry messages dropped by the application after the max number of
// retries was exceeded.
func (nsc *NilStatsCollector) AddDroppedRetries(int) {}

// AddBatchSize records a count of the number of messages attempted by PutRecords in the producer.
func (nsc *NilStatsCollector) AddBatchSize(int) {}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (nsc *NilStatsCollector) AddPutRecordsProvisionedThroughputExceeded(int) {}

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (nsc *NilStatsCollector) AddPutRecordsCalled(int) {}

// AddProvisionedThroughputExceeded records the number of times the PutRecords API response contained a record which
// contained an ErrCodeProvisionedThroughputExceededException error.
func (nsc *NilStatsCollector) AddProvisionedThroughputExceeded(int) {}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (nsc *NilStatsCollector) AddPutRecordsTimeout(int) {}

// AddPutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (nsc *NilStatsCollector) AddPutRecordsDuration(time.Duration) {}

// AddPutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (nsc *NilStatsCollector) AddPutRecordsBuildDuration(time.Duration) {}

// AddPutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (nsc *NilStatsCollector) AddPutRecordsSendDuration(time.Duration) {}

// DefaultStatsCollector is a type that implements the producers's StatsCollector interface using the
// rcrowley/go-metrics library
type DefaultStatsCollector struct {
	Sent                                    metrics.Counter
	Failed                                  metrics.Counter
	DroppedTotal                            metrics.Counter
	DroppedCapacity                         metrics.Counter
	DroppedRetries                          metrics.Counter
	BatchSize                               metrics.Counter
	PutRecordsProvisionedThroughputExceeded metrics.Counter
	PutRecordsCalled                        metrics.Counter
	ProvisionedThroughputExceeded           metrics.Counter
	PutRecordsTimeout                       metrics.Counter
	PutRecordsDuration                      metrics.Gauge
	PutRecordsBuildDuration                 metrics.Gauge
	PutRecordsSendDuration                  metrics.Gauge
}

// NewDefaultStatsCollector instantiates a new DefaultStatsCollector object
func NewDefaultStatsCollector(r metrics.Registry) *DefaultStatsCollector {
	return &DefaultStatsCollector{
		Sent:            metrics.GetOrRegisterCounter("producer.sent", r),
		Failed:          metrics.GetOrRegisterCounter("producer.failed", r),
		DroppedTotal:    metrics.GetOrRegisterCounter("producer.dropped.total", r),
		DroppedCapacity: metrics.GetOrRegisterCounter("producer.dropped.capacity", r),
		DroppedRetries:  metrics.GetOrRegisterCounter("producer.dropped.retries", r),
		BatchSize:       metrics.GetOrRegisterCounter("producer.batchsize", r),
		PutRecordsProvisionedThroughputExceeded: metrics.GetOrRegisterCounter("producer.putrecords.provisionedthroughputexceeded", r),
		PutRecordsCalled:                        metrics.GetOrRegisterCounter("producer.putrecords.called", r),
		ProvisionedThroughputExceeded:           metrics.GetOrRegisterCounter("producer.provisionedthroughputexceeded", r),
		PutRecordsTimeout:                       metrics.GetOrRegisterCounter("producer.putrecords.timeout", r),
		PutRecordsDuration:                      metrics.GetOrRegisterGauge("producer.putrecords.duration", r),
		PutRecordsBuildDuration:                 metrics.GetOrRegisterGauge("producer.putrecords.build.duration", r),
		PutRecordsSendDuration:                  metrics.GetOrRegisterGauge("producer.putrecords.send.duration", r),
	}
}

// AddSent records a count of the number of messages sent to AWS Kinesis by the producer.
func (dsc *DefaultStatsCollector) AddSent(count int) {
	dsc.Sent.Inc(int64(count))
}

// AddFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (dsc *DefaultStatsCollector) AddFailed(count int) {
	dsc.Failed.Inc(int64(count))
}

// AddDroppedTotal records a count of the total number of messages dropped by the application after multiple failures.
func (dsc *DefaultStatsCollector) AddDroppedTotal(count int) {
	dsc.DroppedTotal.Inc(int64(count))
}

// AddDroppedCapacity records a count of the number of messages that were dropped by the application due to the stream
// writer being at capacity.
func (dsc *DefaultStatsCollector) AddDroppedCapacity(count int) {
	dsc.DroppedCapacity.Inc(int64(count))
}

// AddDroppedRetries records a count of the number of retry messages dropped by the application after the max number of
// retries was exceeded.
func (dsc *DefaultStatsCollector) AddDroppedRetries(count int) {
	dsc.DroppedRetries.Inc(int64(count))
}

// AddBatchSize records a count of the number of messages attempted by PutRecords in the producer.
func (dsc *DefaultStatsCollector) AddBatchSize(count int) {
	dsc.BatchSize.Inc(int64(count))
}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (dsc *DefaultStatsCollector) AddPutRecordsProvisionedThroughputExceeded(count int) {
	dsc.PutRecordsProvisionedThroughputExceeded.Inc(int64(count))
}

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (dsc *DefaultStatsCollector) AddPutRecordsCalled(count int) {
	dsc.PutRecordsCalled.Inc(int64(count))
}

// AddProvisionedThroughputExceeded records the number of times the PutRecords API response contained a record which
// contained an ErrCodeProvisionedThroughputExceededException error.
func (dsc *DefaultStatsCollector) AddProvisionedThroughputExceeded(count int) {
	dsc.ProvisionedThroughputExceeded.Inc(int64(count))
}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (dsc *DefaultStatsCollector) AddPutRecordsTimeout(count int) {
	dsc.PutRecordsTimeout.Inc(int64(count))
}

// AddPutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (dsc *DefaultStatsCollector) AddPutRecordsDuration(duration time.Duration) {
	dsc.PutRecordsDuration.Update(duration.Nanoseconds())
}

// AddPutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (dsc *DefaultStatsCollector) AddPutRecordsBuildDuration(duration time.Duration) {
	dsc.PutRecordsBuildDuration.Update(duration.Nanoseconds())
}

// AddPutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (dsc *DefaultStatsCollector) AddPutRecordsSendDuration(duration time.Duration) {
	dsc.PutRecordsSendDuration.Update(duration.Nanoseconds())
}

// PrintStats logs the stats
func (dsc *DefaultStatsCollector) PrintStats() {
	log.Printf("Producer Stats: Sent: [%d]\n", dsc.Sent.Count())
	log.Printf("Producer Stats: Failed: [%d]\n", dsc.Failed.Count())
	log.Printf("Producer Stats: Dropped Total: [%d]\n", dsc.DroppedTotal.Count())
	log.Printf("Producer Stats: Dropped Retries: [%d]\n", dsc.DroppedRetries.Count())
	log.Printf("Producer Stats: Dropped Capacity: [%d]\n", dsc.DroppedCapacity.Count())
	log.Printf("Producer Stats: Batch Size: [%d]\n", dsc.BatchSize.Count())
	log.Printf("Producer Stats: PutRecords Called: [%d]\n", dsc.PutRecordsCalled.Count())
	log.Printf("Producer Stats: Provisioned Throughput Exceeded: [%d]\n", dsc.ProvisionedThroughputExceeded.Count())
	log.Printf("Producer Stats: PutRecords Timeout: [%d]\n", dsc.PutRecordsTimeout.Count())
}

