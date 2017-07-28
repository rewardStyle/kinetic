package kinetic

import (
	"log"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

// ProducerStatsCollector allows for a collector to collect various metrics produced by
// the Kinetic producer library.  This was really built with rcrowley/go-metrics
// in mind.
type ProducerStatsCollector interface {
	AddSentTotal(int)
	AddSentSuccess(int)
	AddSentFailed(int)
	AddSentRetried(int)
	AddDroppedTotal(int)
	AddDroppedCapacity(int)
	AddDroppedRetries(int)
	AddPutRecordsCalled(int)
	AddPutRecordsTimeout(int)
	AddPutRecordsProvisionedThroughputExceeded(int)
	UpdatePutRecordsDuration(time.Duration)
	UpdatePutRecordsBuildDuration(time.Duration)
	UpdatePutRecordsSendDuration(time.Duration)
	UpdateProducerConcurrency(int)
}

// NilProducerStatsCollector is a stats consumer that ignores all metrics.
type NilProducerStatsCollector struct{}

// AddSentTotal records a count of the total number of messages attempted by PutRecords in the producer.
func (nsc *NilProducerStatsCollector) AddSentTotal(int) {}

// AddSentSuccess records a count of the number of messages sent successfully to AWS Kinesis by the producer.
func (nsc *NilProducerStatsCollector) AddSentSuccess(int) {}

// AddSentFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (nsc *NilProducerStatsCollector) AddSentFailed(int) {}

// AddSentRetried records a count of the number of messages that were retried after some error occurred when sending
// to AWS Kinesis by the producer.
func (nsc *NilProducerStatsCollector) AddSentRetried(int) {}

// AddDroppedTotal records a count of the total number of messages dropped by the application after multiple failures.
func (nsc *NilProducerStatsCollector) AddDroppedTotal(int) {}

// AddDroppedCapacity records a count of the number of messages that were dropped by the application due to the stream
// writer being at capacity.
func (nsc *NilProducerStatsCollector) AddDroppedCapacity(int) {}

// AddDroppedRetries records a count of the number of retry messages dropped by the application after the max number of
// retries was exceeded.
func (nsc *NilProducerStatsCollector) AddDroppedRetries(int) {}

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (nsc *NilProducerStatsCollector) AddPutRecordsCalled(int) {}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (nsc *NilProducerStatsCollector) AddPutRecordsTimeout(int) {}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (nsc *NilProducerStatsCollector) AddPutRecordsProvisionedThroughputExceeded(int) {}

// UpdatePutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (nsc *NilProducerStatsCollector) UpdatePutRecordsDuration(time.Duration) {}

// UpdatePutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (nsc *NilProducerStatsCollector) UpdatePutRecordsBuildDuration(time.Duration) {}

// UpdatePutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (nsc *NilProducerStatsCollector) UpdatePutRecordsSendDuration(time.Duration) {}

// UpdateProducerConcurrency records the number of concurrent workers that the producer has.
func (nsc *NilProducerStatsCollector) UpdateProducerConcurrency(int) {}

// Metric names to be exported
const (
	MetricsSentTotal                               = "kinetic.producer.sent.total"
	MetricsSentSuccess                             = "kinetic.producer.sent.success"
	MetricsSentFailed                              = "kinetic.producer.sent.failed"
	MetricsSentRetried                             = "kinetic.producer.sent.retried"
	MetricsDroppedTotal                            = "kinetic.producer.dropped.total"
	MetricsDroppedCapacity                         = "kinetic.producer.dropped.capacity"
	MetricsDroppedRetries                          = "kinetic.producer.dropped.retries"
	MetricsPutRecordsCalled                        = "kinetic.producer.putrecords.called"
	MetricsPutRecordsTimeout                       = "kinetic.producer.putrecords.timeout"
	MetricsPutRecordsProvisionedThroughputExceeded = "kinetic.producer.putrecords.provisionedthroughputexceeded"
	MetricsPutRecordsDuration                      = "kinetic.producer.putrecords.duration"
	MetricsPutRecordsBuildDuration                 = "kinetic.producer.putrecords.build.duration"
	MetricsPutRecordsSendDuration                  = "kinetic.producer.putrecords.send.duration"
	MetricsProducerConcurrency                     = "kinetic.producer.concurrency"
)

// DefaultProducerStatsCollector is a type that implements the producers's StatsCollector interface using the
// rcrowley/go-metrics library
type DefaultProducerStatsCollector struct {
	SentTotal                               metrics.Counter
	SentSuccess                             metrics.Counter
	SentFailed                              metrics.Counter
	SentRetried                             metrics.Counter
	DroppedTotal                            metrics.Counter
	DroppedCapacity                         metrics.Counter
	DroppedRetries                          metrics.Counter
	PutRecordsCalled                        metrics.Counter
	PutRecordsTimeout                       metrics.Counter
	PutRecordsProvisionedThroughputExceeded metrics.Counter
	PutRecordsDuration                      metrics.Gauge
	PutRecordsBuildDuration                 metrics.Gauge
	PutRecordsSendDuration                  metrics.Gauge
	ProducerConcurrency                     metrics.Gauge
}

// NewDefaultProducerStatsCollector instantiates a new DefaultStatsCollector object
func NewDefaultProducerStatsCollector(r metrics.Registry) *DefaultProducerStatsCollector {
	return &DefaultProducerStatsCollector{
		SentTotal:                               metrics.GetOrRegisterCounter(MetricsSentTotal, r),
		SentSuccess:                             metrics.GetOrRegisterCounter(MetricsSentSuccess, r),
		SentFailed:                              metrics.GetOrRegisterCounter(MetricsSentFailed, r),
		SentRetried:                             metrics.GetOrRegisterCounter(MetricsSentRetried, r),
		DroppedTotal:                            metrics.GetOrRegisterCounter(MetricsDroppedTotal, r),
		DroppedCapacity:                         metrics.GetOrRegisterCounter(MetricsDroppedCapacity, r),
		DroppedRetries:                          metrics.GetOrRegisterCounter(MetricsDroppedRetries, r),
		PutRecordsCalled:                        metrics.GetOrRegisterCounter(MetricsPutRecordsCalled, r),
		PutRecordsTimeout:                       metrics.GetOrRegisterCounter(MetricsPutRecordsTimeout, r),
		PutRecordsProvisionedThroughputExceeded: metrics.GetOrRegisterCounter(MetricsPutRecordsProvisionedThroughputExceeded, r),
		PutRecordsDuration:                      metrics.GetOrRegisterGauge(MetricsPutRecordsDuration, r),
		PutRecordsBuildDuration:                 metrics.GetOrRegisterGauge(MetricsPutRecordsBuildDuration, r),
		PutRecordsSendDuration:                  metrics.GetOrRegisterGauge(MetricsPutRecordsSendDuration, r),
		ProducerConcurrency:                     metrics.GetOrRegisterGauge(MetricsProducerConcurrency, r),
	}
}

// AddSentTotal records a count of the total number of messages attempted by PutRecords in the producer.
func (dsc *DefaultProducerStatsCollector) AddSentTotal(count int) {
	dsc.SentTotal.Inc(int64(count))
}

// AddSentSuccess records a count of the number of messages sent successfully to AWS Kinesis by the producer.
func (dsc *DefaultProducerStatsCollector) AddSentSuccess(count int) {
	dsc.SentSuccess.Inc(int64(count))
}

// AddSentFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (dsc *DefaultProducerStatsCollector) AddSentFailed(count int) {
	dsc.SentFailed.Inc(int64(count))
}

// AddSentRetried records a count of the number of messages that were retried after some error occurred when sending
// to AWS Kinesis by the producer.
func (dsc *DefaultProducerStatsCollector) AddSentRetried(count int) {
	dsc.SentRetried.Inc(int64(count))
}

// AddDroppedTotal records a count of the total number of messages dropped by the application after multiple failures.
func (dsc *DefaultProducerStatsCollector) AddDroppedTotal(count int) {
	dsc.DroppedTotal.Inc(int64(count))
}

// AddDroppedCapacity records a count of the number of messages that were dropped by the application due to the stream
// writer being at capacity.
func (dsc *DefaultProducerStatsCollector) AddDroppedCapacity(count int) {
	dsc.DroppedCapacity.Inc(int64(count))
}

// AddDroppedRetries records a count of the number of retry messages dropped by the application after the max number of
// retries was exceeded.
func (dsc *DefaultProducerStatsCollector) AddDroppedRetries(count int) {
	dsc.DroppedRetries.Inc(int64(count))
}

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (dsc *DefaultProducerStatsCollector) AddPutRecordsCalled(count int) {
	dsc.PutRecordsCalled.Inc(int64(count))
}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (dsc *DefaultProducerStatsCollector) AddPutRecordsTimeout(count int) {
	dsc.PutRecordsTimeout.Inc(int64(count))
}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (dsc *DefaultProducerStatsCollector) AddPutRecordsProvisionedThroughputExceeded(count int) {
	dsc.PutRecordsProvisionedThroughputExceeded.Inc(int64(count))
}

// UpdatePutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (dsc *DefaultProducerStatsCollector) UpdatePutRecordsDuration(duration time.Duration) {
	dsc.PutRecordsDuration.Update(duration.Nanoseconds())
}

// UpdatePutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (dsc *DefaultProducerStatsCollector) UpdatePutRecordsBuildDuration(duration time.Duration) {
	dsc.PutRecordsBuildDuration.Update(duration.Nanoseconds())
}

// UpdatePutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (dsc *DefaultProducerStatsCollector) UpdatePutRecordsSendDuration(duration time.Duration) {
	dsc.PutRecordsSendDuration.Update(duration.Nanoseconds())
}

// UpdateProducerConcurrency records the number of concurrent workers that the producer has.
func (dsc *DefaultProducerStatsCollector) UpdateProducerConcurrency(count int) {
	dsc.ProducerConcurrency.Update(int64(count))
}

// PrintStats logs the stats
func (dsc *DefaultProducerStatsCollector) PrintStats() {
	log.Printf("Producer Stats: Sent Total: [%d]\n", dsc.SentTotal.Count())
	log.Printf("Producer Stats: Sent Success: [%d]\n", dsc.SentSuccess.Count())
	log.Printf("Producer Stats: Sent Failed: [%d]\n", dsc.SentFailed.Count())
	log.Printf("Producer Stats: Sent Retried: [%d]\n", dsc.SentRetried.Count())
	log.Printf("Producer Stats: Dropped Total: [%d]\n", dsc.DroppedTotal.Count())
	log.Printf("Producer Stats: Dropped Retries: [%d]\n", dsc.DroppedRetries.Count())
	log.Printf("Producer Stats: Dropped Capacity: [%d]\n", dsc.DroppedCapacity.Count())
	log.Printf("Producer Stats: PutRecords Called: [%d]\n", dsc.PutRecordsCalled.Count())
	log.Printf("Producer Stats: PutRecords Timeout: [%d]\n", dsc.PutRecordsTimeout.Count())
	log.Printf("Producer Stats: PutRecords Provisioned Throughput Exceeded: [%d]\n", dsc.PutRecordsProvisionedThroughputExceeded.Count())
	log.Printf("Producer Stats: PutRecords Duration (ns): [%d]\n", dsc.PutRecordsDuration.Value())
	log.Printf("Producer Stats: PutRecords Build Duration (ns): [%d]\n", dsc.PutRecordsBuildDuration.Value())
	log.Printf("Producer Stats: PutRecords Send Duration (ns): [%d]\n", dsc.PutRecordsSendDuration.Value())
	log.Printf("Producer Stats: Producer Concurrency: [%d]\n", dsc.ProducerConcurrency.Value())
}
