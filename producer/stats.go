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
	AddSentTotal(int)
	AddSentSuccess(int)
	AddSentFailed(int)
	AddSentRetried(int)
	AddDroppedTotal(int)
	AddDroppedCapacity(int)
	AddDroppedRetries(int)
	AddPutRecordsCalled(int)
	AddPutRecordsTimeout(int)
	AddProvisionedThroughputExceeded(int)
	AddPutRecordsProvisionedThroughputExceeded(int)
	UpdatePutRecordsDuration(time.Duration)
	UpdatePutRecordsBuildDuration(time.Duration)
	UpdatePutRecordsSendDuration(time.Duration)
	UpdateProducerConcurrency(int)
}

// NilStatsCollector is a stats listener that ignores all metrics.
type NilStatsCollector struct{}

// AddSentTotal records a count of the total number of messages attempted by PutRecords in the producer.
func (nsc *NilStatsCollector) AddSentTotal(int) {}

// AddSentSuccess records a count of the number of messages sent successfully to AWS Kinesis by the producer.
func (nsc *NilStatsCollector) AddSentSuccess(int) {}

// AddSentFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (nsc *NilStatsCollector) AddSentFailed(int) {}

// AddSentRetried records a count of the number of messages that were retried after some error occurred when sending
// to AWS Kinesis by the producer.
func (nsc *NilStatsCollector) AddSentRetried(int) {}

// AddDroppedTotal records a count of the total number of messages dropped by the application after multiple failures.
func (nsc *NilStatsCollector) AddDroppedTotal(int) {}

// AddDroppedCapacity records a count of the number of messages that were dropped by the application due to the stream
// writer being at capacity.
func (nsc *NilStatsCollector) AddDroppedCapacity(int) {}

// AddDroppedRetries records a count of the number of retry messages dropped by the application after the max number of
// retries was exceeded.
func (nsc *NilStatsCollector) AddDroppedRetries(int) {}

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (nsc *NilStatsCollector) AddPutRecordsCalled(int) {}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (nsc *NilStatsCollector) AddPutRecordsTimeout(int) {}

// AddProvisionedThroughputExceeded records the number of times the PutRecords API response contained a record which
// contained an ErrCodeProvisionedThroughputExceededException error.
func (nsc *NilStatsCollector) AddProvisionedThroughputExceeded(int) {}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (nsc *NilStatsCollector) AddPutRecordsProvisionedThroughputExceeded(int) {}

// UpdatePutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (nsc *NilStatsCollector) UpdatePutRecordsDuration(time.Duration) {}

// UpdatePutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (nsc *NilStatsCollector) UpdatePutRecordsBuildDuration(time.Duration) {}

// UpdatePutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (nsc *NilStatsCollector) UpdatePutRecordsSendDuration(time.Duration) {}

// UpdateProducerConcurrency records the number of concurrent workers that the producer has.
func (nsc *NilStatsCollector) UpdateProducerConcurrency(int) {}

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
	MetricsProvisionedThroughputExceeded           = "kinetic.producer.provisionedthroughputexceeded"
	MetricsPutRecordsProvisionedThroughputExceeded = "kinetic.producer.putrecords.provisionedthroughputexceeded"
	MetricsPutRecordsDuration                      = "kinetic.producer.putrecords.duration"
	MetricsPutRecordsBuildDuration                 = "kinetic.producer.putrecords.build.duration"
	MetricsPutRecordsSendDuration                  = "kinetic.producer.putrecords.send.duration"
	MetricsProducerConcurrency                     = "kinetic.producer.concurrency"
)

// DefaultStatsCollector is a type that implements the producers's StatsCollector interface using the
// rcrowley/go-metrics library
type DefaultStatsCollector struct {
	SentTotal                               metrics.Counter
	SentSuccess                             metrics.Counter
	SentFailed                              metrics.Counter
	SentRetried                             metrics.Counter
	DroppedTotal                            metrics.Counter
	DroppedCapacity                         metrics.Counter
	DroppedRetries                          metrics.Counter
	PutRecordsCalled                        metrics.Counter
	PutRecordsTimeout                       metrics.Counter
	ProvisionedThroughputExceeded           metrics.Counter
	PutRecordsProvisionedThroughputExceeded metrics.Counter
	PutRecordsDuration                      metrics.Gauge
	PutRecordsBuildDuration                 metrics.Gauge
	PutRecordsSendDuration                  metrics.Gauge
	ProducerConcurrency                     metrics.Gauge
}

// NewDefaultStatsCollector instantiates a new DefaultStatsCollector object
func NewDefaultStatsCollector(r metrics.Registry) *DefaultStatsCollector {
	return &DefaultStatsCollector{
		SentTotal:                               metrics.GetOrRegisterCounter(MetricsSentTotal, r),
		SentSuccess:                             metrics.GetOrRegisterCounter(MetricsSentSuccess, r),
		SentFailed:                              metrics.GetOrRegisterCounter(MetricsSentFailed, r),
		SentRetried:                             metrics.GetOrRegisterCounter(MetricsSentRetried, r),
		DroppedTotal:                            metrics.GetOrRegisterCounter(MetricsDroppedTotal, r),
		DroppedCapacity:                         metrics.GetOrRegisterCounter(MetricsDroppedCapacity, r),
		DroppedRetries:                          metrics.GetOrRegisterCounter(MetricsDroppedRetries, r),
		PutRecordsCalled:                        metrics.GetOrRegisterCounter(MetricsPutRecordsCalled, r),
		PutRecordsTimeout:                       metrics.GetOrRegisterCounter(MetricsPutRecordsTimeout, r),
		ProvisionedThroughputExceeded:           metrics.GetOrRegisterCounter(MetricsProvisionedThroughputExceeded, r),
		PutRecordsProvisionedThroughputExceeded: metrics.GetOrRegisterCounter(MetricsPutRecordsProvisionedThroughputExceeded, r),
		PutRecordsDuration:                      metrics.GetOrRegisterGauge(MetricsPutRecordsDuration, r),
		PutRecordsBuildDuration:                 metrics.GetOrRegisterGauge(MetricsPutRecordsBuildDuration, r),
		PutRecordsSendDuration:                  metrics.GetOrRegisterGauge(MetricsPutRecordsSendDuration, r),
		ProducerConcurrency:                     metrics.GetOrRegisterGauge(MetricsProducerConcurrency, r),
	}
}

// AddSentTotal records a count of the total number of messages attempted by PutRecords in the producer.
func (dsc *DefaultStatsCollector) AddSentTotal(count int) {
	dsc.SentTotal.Inc(int64(count))
}

// AddSentSuccess records a count of the number of messages sent successfully to AWS Kinesis by the producer.
func (dsc *DefaultStatsCollector) AddSentSuccess(count int) {
	dsc.SentSuccess.Inc(int64(count))
}

// AddSentFailed records a count of the number of messages that failed to be sent to AWS Kinesis by the producer.
func (dsc *DefaultStatsCollector) AddSentFailed(count int) {
	dsc.SentFailed.Inc(int64(count))
}

// AddSentRetried records a count of the number of messages that were retried after some error occurred when sending
// to AWS Kinesis by the producer.
func (dsc *DefaultStatsCollector) AddSentRetried(count int) {
	dsc.SentRetried.Inc(int64(count))
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

// AddPutRecordsCalled records the number of times the PutRecords API was called by the producer.
func (dsc *DefaultStatsCollector) AddPutRecordsCalled(count int) {
	dsc.PutRecordsCalled.Inc(int64(count))
}

// AddPutRecordsTimeout records the number of times the PutRecords API timed out on the HTTP level.  This is influenced
// by the WithHTTPClientTimeout configuration.
func (dsc *DefaultStatsCollector) AddPutRecordsTimeout(count int) {
	dsc.PutRecordsTimeout.Inc(int64(count))
}

// AddProvisionedThroughputExceeded records the number of times the PutRecords API response contained a record which
// contained an ErrCodeProvisionedThroughputExceededException error.
func (dsc *DefaultStatsCollector) AddProvisionedThroughputExceeded(count int) {
	dsc.ProvisionedThroughputExceeded.Inc(int64(count))
}

// AddPutRecordsProvisionedThroughputExceeded records the number of times the PutRecords API returned a
// ErrCodeProvisionedThroughputExceededException by the producer.
func (dsc *DefaultStatsCollector) AddPutRecordsProvisionedThroughputExceeded(count int) {
	dsc.PutRecordsProvisionedThroughputExceeded.Inc(int64(count))
}

// UpdatePutRecordsDuration records the duration that the PutRecords API request took.  Only the times of successful calls
// are measured.
func (dsc *DefaultStatsCollector) UpdatePutRecordsDuration(duration time.Duration) {
	dsc.PutRecordsDuration.Update(duration.Nanoseconds())
}

// UpdatePutRecordsBuildDuration records the duration that it took to build the PutRecords API request payload.
func (dsc *DefaultStatsCollector) UpdatePutRecordsBuildDuration(duration time.Duration) {
	dsc.PutRecordsBuildDuration.Update(duration.Nanoseconds())
}

// UpdatePutRecordsSendDuration records the duration that it took to send the PutRecords API request payload.
func (dsc *DefaultStatsCollector) UpdatePutRecordsSendDuration(duration time.Duration) {
	dsc.PutRecordsSendDuration.Update(duration.Nanoseconds())
}

// UpdateProducerConcurrency records the number of concurrent workers that the producer has.
func (dsc *DefaultStatsCollector) UpdateProducerConcurrency(count int) {
	dsc.ProducerConcurrency.Update(int64(count))
}

// PrintStats logs the stats
func (dsc *DefaultStatsCollector) PrintStats() {
	log.Printf("Producer Stats: Sent Total: [%d]\n", dsc.SentTotal.Count())
	log.Printf("Producer Stats: Sent Success: [%d]\n", dsc.SentSuccess.Count())
	log.Printf("Producer Stats: Sent Failed: [%d]\n", dsc.SentFailed.Count())
	log.Printf("Producer Stats: Sent Retried: [%d]\n", dsc.SentRetried.Count())
	log.Printf("Producer Stats: Dropped Total: [%d]\n", dsc.DroppedTotal.Count())
	log.Printf("Producer Stats: Dropped Retries: [%d]\n", dsc.DroppedRetries.Count())
	log.Printf("Producer Stats: Dropped Capacity: [%d]\n", dsc.DroppedCapacity.Count())
	log.Printf("Producer Stats: PutRecords Called: [%d]\n", dsc.PutRecordsCalled.Count())
	log.Printf("Producer Stats: PutRecords Timeout: [%d]\n", dsc.PutRecordsTimeout.Count())
	log.Printf("Producer Stats: Provisioned Throughput Exceeded: [%d]\n", dsc.ProvisionedThroughputExceeded.Count())
	log.Printf("Producer Stats: PutRecords Provisioned Throughput Exceeded: [%d]\n", dsc.PutRecordsProvisionedThroughputExceeded.Count())
	log.Printf("Producer Stats: PutRecords Duration (ns): [%d]\n", dsc.PutRecordsDuration.Value())
	log.Printf("Producer Stats: PutRecords Build Duration (ns): [%d]\n", dsc.PutRecordsBuildDuration.Value())
	log.Printf("Producer Stats: PutRecords Send Duration (ns): [%d]\n", dsc.PutRecordsSendDuration.Value())
	log.Printf("Producer Stats: Producer Concurrency: [%d]\n", dsc.ProducerConcurrency.Value())
}
