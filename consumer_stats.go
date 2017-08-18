package kinetic

import (
	"log"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

// ConsumerStatsCollector allows for a collector to collect various metrics produced by
// the Kinetic Consumer library.  This was really built with rcrowley/go-metrics
// in mind.
type ConsumerStatsCollector interface {
	AddConsumed(int)
	AddDelivered(int)
	AddProcessed(int)
	UpdateBatchSize(int)
	AddGetRecordsCalled(int)
	AddReadProvisionedThroughputExceeded(int)
	AddGetRecordsTimeout(int)
	AddGetRecordsReadTimeout(int)
	UpdateProcessedDuration(time.Duration)
	UpdateGetRecordsDuration(time.Duration)
	UpdateGetRecordsReadResponseDuration(time.Duration)
	UpdateGetRecordsUnmarshalDuration(time.Duration)
	AddCheckpointInsert(int)
	AddCheckpointDone(int)
	UpdateCheckpointSize(int)
	AddCheckpointSent(int)
	AddCheckpointSuccess(int)
	AddCheckpointError(int)
}

// NilConsumerStatsCollector is a stats consumer that ignores all metrics.
type NilConsumerStatsCollector struct{}

// AddConsumed records a count of the number of messages received from AWS
// Kinesis by the consumer.
func (nsc *NilConsumerStatsCollector) AddConsumed(int) {}

// AddDelivered records a count of the number of messages delivered to the
// application by the consumer.
func (nsc *NilConsumerStatsCollector) AddDelivered(int) {}

// AddProcessed records a count of the number of messages processed by the
// application by the consumer.  This is based on a WaitGroup that is sent to
// the RetrieveFn and Listen functions.  Retrieve does not count processed
// messages.
func (nsc *NilConsumerStatsCollector) AddProcessed(int) {}

// UpdateBatchSize records a count of the number of messages returned by
// GetRecords in the consumer.
func (nsc *NilConsumerStatsCollector) UpdateBatchSize(int) {}

// AddGetRecordsCalled records the number of times the GetRecords API was called
// by the consumer.
func (nsc *NilConsumerStatsCollector) AddGetRecordsCalled(int) {}

// AddReadProvisionedThroughputExceeded records the number of times the GetRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the consumer.
func (nsc *NilConsumerStatsCollector) AddReadProvisionedThroughputExceeded(int) {}

// AddGetRecordsTimeout records the number of times the GetRecords API timed out
// on the HTTP level.  This is influenced by the WithHTTPClientTimeout
// configuration.
func (nsc *NilConsumerStatsCollector) AddGetRecordsTimeout(int) {}

// AddGetRecordsReadTimeout records the number of times the GetRecords API timed
// out while reading the response body.  This is influenced by the
// WithGetRecordsReadTimeout configuration.
func (nsc *NilConsumerStatsCollector) AddGetRecordsReadTimeout(int) {}

// UpdateProcessedDuration records the duration to process a record.  See notes on
// AddProcessed.
func (nsc *NilConsumerStatsCollector) UpdateProcessedDuration(time.Duration) {}

// UpdateGetRecordsDuration records the duration that the GetRecords API request
// took.  Only the times of successful calls are measured.
func (nsc *NilConsumerStatsCollector) UpdateGetRecordsDuration(time.Duration) {}

// UpdateGetRecordsReadResponseDuration records the duration that it took to read
// the response body of a GetRecords API request.
func (nsc *NilConsumerStatsCollector) UpdateGetRecordsReadResponseDuration(time.Duration) {}

// UpdateGetRecordsUnmarshalDuration records the duration that it took to unmarshal
// the response body of a GetRecords API request.
func (nsc *NilConsumerStatsCollector) UpdateGetRecordsUnmarshalDuration(time.Duration) {}

// AddCheckpointInsert records the number of times the CheckpointInsert API was called.
func (nsc *NilConsumerStatsCollector) AddCheckpointInsert(int) {}

// AddCheckpointDone records the number of times the CheckpointDone API was called.
func (nsc *NilConsumerStatsCollector) AddCheckpointDone(int) {}

// UpdateCheckpointSize records the current size of the checkpointer.
func (nsc *NilConsumerStatsCollector) UpdateCheckpointSize(int) {}

// AddCheckpointSent records the number of times a checkpoint action message was sent to KCL.
func (nsc *NilConsumerStatsCollector) AddCheckpointSent(int) {}

// AddCheckpointSuccess records the number of times KCL send a checkpoint acknowledgement indicating that
// checkpointing was successful
func (nsc *NilConsumerStatsCollector) AddCheckpointSuccess(int) {}

// AddCheckpointError records the number of times KCL send a checkpoint acknowledgement indicating that
// checkpointing was not successful
func (nsc *NilConsumerStatsCollector) AddCheckpointError(int) {}

// Metric names to be exported
const (
	MetricsConsumed                          = "kinetic.consumer.consumed"
	MetricsDelivered                         = "kinetic.consumer.delivered"
	MetricsProcessed                         = "kinetic.consumer.processed"
	MetricsBatchSize                         = "kinetic.consumer.batchsize"
	MetricsSent                              = "kinetic.consumer.sent"
	MetricsReadProvisionedThroughputExceeded = "kinetic.consumer.getrecords.provisionedthroughputexceeded"
	MetricsGetRecordsTimeout                 = "kinetic.consumer.getrecords.timeout"
	MetricsGetRecordsReadTimeout             = "kinetic.consumer.getrecords.readtimeout"
	MetricsProcessedDuration                 = "kinetic.consumer.processed.duration"
	MetricsGetRecordsDuration                = "kinetic.consumer.getrecords.duration"
	MetricsGetRecordsReadResponseDuration    = "kinetic.consumer.getrecords.readresponse.duration"
	MetricsGetRecordsUnmarshalDuration       = "kinetic.consumer.getrecords.unmarshal.duration"
	MetricsCheckpointInsert                  = "kinetic.consumer.checkpoint.insert"
	MetricsCheckpointDone                    = "kinetic.consumer.checkpoint.done"
	MetricsCheckpointSize                    = "kinetic.consumer.checkpoint.size"
	MetricsCheckpointSent                    = "kinetic.consumer.checkpoint.sent"
	MetricsCheckpointSuccess                 = "kinetic.consumer.checkpoint.success"
	MetricsCheckpointError                   = "kinetic.consumer.checkpoint.error"
)

// DefaultConsumerStatsCollector is a type that implements the consumer's StatsCollector interface using the
// rcrowley/go-metrics library
type DefaultConsumerStatsCollector struct {
	Consumed                          metrics.Counter
	Delivered                         metrics.Counter
	Processed                         metrics.Counter
	BatchSize                         metrics.Gauge
	GetRecordsCalled                  metrics.Counter
	ReadProvisionedThroughputExceeded metrics.Counter
	GetRecordsTimeout                 metrics.Counter
	GetRecordsReadTimeout             metrics.Counter
	ProcessedDuration                 metrics.Gauge
	GetRecordsDuration                metrics.Gauge
	GetRecordsReadResponseDuration    metrics.Gauge
	GetRecordsUnmarshalDuration       metrics.Gauge
	CheckpointInsert                  metrics.Counter
	CheckpointDone                    metrics.Counter
	CheckpointSize                    metrics.Gauge
	CheckpointSent                    metrics.Counter
	CheckpointSuccess                 metrics.Counter
	CheckpointError                   metrics.Counter
}

// NewDefaultConsumerStatsCollector instantiates a new DefaultStatsCollector object
func NewDefaultConsumerStatsCollector(r metrics.Registry) *DefaultConsumerStatsCollector {
	return &DefaultConsumerStatsCollector{
		Consumed:                          metrics.GetOrRegisterCounter(MetricsConsumed, r),
		Delivered:                         metrics.GetOrRegisterCounter(MetricsDelivered, r),
		Processed:                         metrics.GetOrRegisterCounter(MetricsProcessed, r),
		BatchSize:                         metrics.GetOrRegisterGauge(MetricsBatchSize, r),
		GetRecordsCalled:                  metrics.GetOrRegisterCounter(MetricsSent, r),
		ReadProvisionedThroughputExceeded: metrics.GetOrRegisterCounter(MetricsReadProvisionedThroughputExceeded, r),
		GetRecordsTimeout:                 metrics.GetOrRegisterCounter(MetricsGetRecordsTimeout, r),
		GetRecordsReadTimeout:             metrics.GetOrRegisterCounter(MetricsGetRecordsReadTimeout, r),
		ProcessedDuration:                 metrics.GetOrRegisterGauge(MetricsProcessedDuration, r),
		GetRecordsDuration:                metrics.GetOrRegisterGauge(MetricsGetRecordsDuration, r),
		GetRecordsReadResponseDuration:    metrics.GetOrRegisterGauge(MetricsGetRecordsReadResponseDuration, r),
		GetRecordsUnmarshalDuration:       metrics.GetOrRegisterGauge(MetricsGetRecordsUnmarshalDuration, r),
		CheckpointInsert:                  metrics.GetOrRegisterCounter(MetricsCheckpointInsert, r),
		CheckpointDone:                    metrics.GetOrRegisterCounter(MetricsCheckpointDone, r),
		CheckpointSize:                    metrics.GetOrRegisterGauge(MetricsCheckpointSize, r),
		CheckpointSent:                    metrics.GetOrRegisterCounter(MetricsCheckpointSent, r),
		CheckpointSuccess:                 metrics.GetOrRegisterCounter(MetricsCheckpointSuccess, r),
		CheckpointError:                   metrics.GetOrRegisterCounter(MetricsCheckpointError, r),
	}
}

// AddConsumed records a count of the number of messages received from AWS
// Kinesis by the consumer.
func (dsc *DefaultConsumerStatsCollector) AddConsumed(count int) {
	dsc.Consumed.Inc(int64(count))
}

// AddDelivered records a count of the number of messages delivered to the
// application by the consumer.
func (dsc *DefaultConsumerStatsCollector) AddDelivered(count int) {
	dsc.Delivered.Inc(int64(count))
}

// AddProcessed records a count of the number of messages processed by the
// application by the consumer.  This is based on a WaitGroup that is sent to
// the RetrieveFn and Listen functions.  Retrieve does not count processed
// messages.
func (dsc *DefaultConsumerStatsCollector) AddProcessed(count int) {
	dsc.Processed.Inc(int64(count))
}

// UpdateBatchSize records a count of the number of messages returned by
// GetRecords in the consumer.
func (dsc *DefaultConsumerStatsCollector) UpdateBatchSize(count int) {
	dsc.BatchSize.Update(int64(count))
}

// AddGetRecordsCalled records the number of times the GetRecords API was called
// by the consumer.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsCalled(count int) {
	dsc.GetRecordsCalled.Inc(int64(count))
}

// AddReadProvisionedThroughputExceeded records the number of times the GetRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the consumer.
func (dsc *DefaultConsumerStatsCollector) AddReadProvisionedThroughputExceeded(count int) {
	dsc.ReadProvisionedThroughputExceeded.Inc(int64(count))
}

// AddGetRecordsTimeout records the number of times the GetRecords API timed out
// on the HTTP level.  This is influenced by the WithHTTPClientTimeout
// configuration.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsTimeout(count int) {
	dsc.GetRecordsTimeout.Inc(int64(count))
}

// AddGetRecordsReadTimeout records the number of times the GetRecords API timed
// out while reading the response body.  This is influenced by the
// WithGetRecordsReadTimeout configuration.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsReadTimeout(count int) {
	dsc.GetRecordsReadTimeout.Inc(int64(count))
}

// UpdateProcessedDuration records the duration to process a record.  See notes on
// AddProcessed.
func (dsc *DefaultConsumerStatsCollector) UpdateProcessedDuration(duration time.Duration) {
	dsc.ProcessedDuration.Update(duration.Nanoseconds())
}

// UpdateGetRecordsDuration records the duration that the GetRecords API request
// took.  Only the times of successful calls are measured.
func (dsc *DefaultConsumerStatsCollector) UpdateGetRecordsDuration(duration time.Duration) {
	dsc.GetRecordsDuration.Update(duration.Nanoseconds())
}

// UpdateGetRecordsReadResponseDuration records the duration that it took to read
// the response body of a GetRecords API request.
func (dsc *DefaultConsumerStatsCollector) UpdateGetRecordsReadResponseDuration(duration time.Duration) {
	dsc.GetRecordsReadResponseDuration.Update(duration.Nanoseconds())
}

// UpdateGetRecordsUnmarshalDuration records the duration that it took to unmarshal
// the response body of a GetRecords API request.
func (dsc *DefaultConsumerStatsCollector) UpdateGetRecordsUnmarshalDuration(duration time.Duration) {
	dsc.GetRecordsUnmarshalDuration.Update(duration.Nanoseconds())
}

// AddCheckpointInsert records the number of times the CheckpointInsert API was called.
func (dsc *DefaultConsumerStatsCollector) AddCheckpointInsert(count int) {
	dsc.CheckpointInsert.Inc(int64(count))
}

// AddCheckpointDone records the number of times the CheckpointDone API was called.
func (dsc *DefaultConsumerStatsCollector) AddCheckpointDone(count int) {
	dsc.CheckpointDone.Inc(int64(count))
}

// UpdateCheckpointSize records the current size of the checkpointer.
func (dsc *DefaultConsumerStatsCollector) UpdateCheckpointSize(size int) {
	dsc.CheckpointSize.Update(int64(size))
}

// AddCheckpointSent records the number of times a checkpoint action message was sent to KCL.
func (dsc *DefaultConsumerStatsCollector) AddCheckpointSent(count int) {
	dsc.CheckpointSent.Inc(int64(count))
}

// AddCheckpointSuccess records the number of times KCL send a checkpoint acknowledgement indicating that
// checkpointing was successful
func (dsc *DefaultConsumerStatsCollector) AddCheckpointSuccess(count int) {
	dsc.CheckpointSuccess.Inc(int64(count))
}

// AddCheckpointError records the number of times KCL send a checkpoint acknowledgement indicating that
// checkpointing was not successful
func (dsc *DefaultConsumerStatsCollector) AddCheckpointError(count int) {
	dsc.CheckpointError.Inc(int64(count))
}

// PrintStats logs the stats
func (dsc *DefaultConsumerStatsCollector) PrintStats() {
	log.Printf("Consumer stats: Consumed: [%d]\n", dsc.Consumed.Count())
	log.Printf("Consumer stats: Delivered: [%d]\n", dsc.Delivered.Count())
	log.Printf("Consumer stats: Processed: [%d]\n", dsc.Processed.Count())
	log.Printf("Consumer stats: Batch Size: [%d]\n", dsc.BatchSize.Value())
	log.Printf("Consumer stats: GetRecords Called: [%d]\n", dsc.GetRecordsCalled.Count())
	log.Printf("Consumer stats: GetRecords Timeout: [%d]\n", dsc.GetRecordsTimeout.Count())
	log.Printf("Consumer stats: GetRecords Read Timeout: [%d]\n", dsc.GetRecordsReadTimeout.Count())
	log.Printf("Consumer stats: GetRecords Provisioned Throughput Exceeded: [%d]\n", dsc.ReadProvisionedThroughputExceeded.Count())
	log.Printf("Consumer stats: Processed Duration (ns): [%d]\n", dsc.ProcessedDuration.Value())
	log.Printf("Consumer stats: GetRecords Duration (ns): [%d]\n", dsc.GetRecordsDuration.Value())
	log.Printf("Consumer stats: GetRecords Read Response Duration (ns): [%d]\n", dsc.GetRecordsReadResponseDuration.Value())
	log.Printf("Consumer stats: GetRecords Unmarshal Duration (ns): [%d]\n", dsc.GetRecordsUnmarshalDuration.Value())
	log.Printf("Consumer stats: Checkpoint Insert: [%d]\n", dsc.CheckpointInsert.Count())
	log.Printf("Consumer stats: Checkpoint Done: [%d]\n", dsc.CheckpointDone.Count())
	log.Printf("Consumer stats: Checkpoint Size: [%d]\n", dsc.CheckpointSize.Value())
	log.Printf("Consumer stats: Checkpoint Sent: [%d]\n", dsc.CheckpointSent.Count())
	log.Printf("Consumer stats: Checkpoint Success: [%d]\n", dsc.CheckpointSuccess.Count())
	log.Printf("Consumer stats: Checkpoint Error: [%d]\n", dsc.CheckpointError.Count())
}
