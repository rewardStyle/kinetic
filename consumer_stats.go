package kinetic

import (
	"log"
	"time"

	metrics "github.com/jasonyurs/go-metrics"
)

// StatsCollector allows for a collector to collect various metrics produced by
// the Kinetic Listener library.  This was really built with rcrowley/go-metrics
// in mind.
type ConsumerStatsCollector interface {
	AddConsumed(int)
	AddDelivered(int)
	AddProcessed(int)
	AddBatchSize(int)
	AddGetRecordsCalled(int)
	AddGetRecordsProvisionedThroughputExceeded(int)
	AddGetRecordsTimeout(int)
	AddGetRecordsReadTimeout(int)
	AddProcessedDuration(time.Duration)
	AddGetRecordsDuration(time.Duration)
	AddGetRecordsReadResponseDuration(time.Duration)
	AddGetRecordsUnmarshalDuration(time.Duration)
}

// NilStatsCollector is a stats listener that ignores all metrics.
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

// AddBatchSize records a count of the number of messages returned by
// GetRecords in the consumer.
func (nsc *NilConsumerStatsCollector) AddBatchSize(int) {}

// AddGetRecordsCalled records the number of times the GetRecords API was called
// by the consumer.
func (nsc *NilConsumerStatsCollector) AddGetRecordsCalled(int) {}

// AddProvisionedThroughputExceeded records the number of times the GetRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the consumer.
func (nsc *NilConsumerStatsCollector) AddGetRecordsProvisionedThroughputExceeded(int) {}

// AddGetRecordsTimeout records the number of times the GetRecords API timed out
// on the HTTP level.  This is influenced by the WithHTTPClientTimeout
// configuration.
func (nsc *NilConsumerStatsCollector) AddGetRecordsTimeout(int) {}

// AddGetRecordsReadTimeout records the number of times the GetRecords API timed
// out while reading the response body.  This is influenced by the
// WithGetRecordsReadTimeout configuration.
func (nsc *NilConsumerStatsCollector) AddGetRecordsReadTimeout(int) {}

// AddProcessedDuration records the duration to process a record.  See notes on
// AddProcessed.
func (nsc *NilConsumerStatsCollector) AddProcessedDuration(time.Duration) {}

// AddGetRecordsDuration records the duration that the GetRecords API request
// took.  Only the times of successful calls are measured.
func (nsc *NilConsumerStatsCollector) AddGetRecordsDuration(time.Duration) {}

// AddGetRecordsReadResponseDuration records the duration that it took to read
// the response body of a GetRecords API request.
func (nsc *NilConsumerStatsCollector) AddGetRecordsReadResponseDuration(time.Duration) {}

// AddGetRecordsUnmarshalDuration records the duration that it took to unmarshal
// the response body of a GetRecords API request.
func (nsc *NilConsumerStatsCollector) AddGetRecordsUnmarshalDuration(time.Duration) {}

// Metric names to be exported
const (
	MetricsConsumed                       	       = "kinetic.consumer.consumed"
	MetricsDelivered                      	       = "kinetic.consumer.delivered"
	MetricsProcessed                      	       = "kinetic.consumer.processed"
	MetricsBatchSize                      	       = "kinetic.consumer.batchsize"
	MetricsSent                           	       = "kinetic.consumer.sent"
	MetricsGetRecordsProvisionedThroughputExceeded = "kinetic.consumer.getrecords.provisionedthroughputexceeded"
	MetricsGetRecordsTimeout              	       = "kinetic.consumer.getrecords.timeout"
	MetricsGetRecordsReadTimeout          	       = "kinetic.consumer.getrecords.readtimeout"
	MetricsProcessedDuration              	       = "kinetic.consumer.processed.duration"
	MetricsGetRecordsDuration             	       = "kinetic.consumer.getrecords.duration"
	MetricsGetRecordsReadResponseDuration 	       = "kinetic.consumer.getrecords.readresponse.duration"
	MetricsGetRecordsUnmarshalDuration    	       = "kinetic.consumer.getrecords.unmarshal.duration"
)

// DefaultStatsCollector is a type that implements the listener's StatsCollector interface using the
// rcrowley/go-metrics library
type DefaultConsumerStatsCollector struct {
	Consumed                                metrics.Counter
	Delivered                               metrics.Counter
	Processed                               metrics.Counter
	BatchSize                               metrics.Counter
	GetRecordsCalled                        metrics.Counter
	GetRecordsProvisionedThroughputExceeded metrics.Counter
	GetRecordsTimeout                       metrics.Counter
	GetRecordsReadTimeout                   metrics.Counter
	ProcessedDuration                       metrics.Gauge
	GetRecordsDuration                      metrics.Gauge
	GetRecordsReadResponseDuration          metrics.Gauge
	GetRecordsUnmarshalDuration             metrics.Gauge
}

// NewDefaultStatsCollector instantiates a new DefaultStatsCollector object
func NewDefaultConsumerStatsCollector(r metrics.Registry) *DefaultConsumerStatsCollector {
	return &DefaultConsumerStatsCollector{
		Consumed:                       	  metrics.GetOrRegisterCounter(MetricsConsumed, r),
		Delivered:                      	  metrics.GetOrRegisterCounter(MetricsDelivered, r),
		Processed:                     	   	  metrics.GetOrRegisterCounter(MetricsProcessed, r),
		BatchSize:                      	  metrics.GetOrRegisterCounter(MetricsBatchSize, r),
		GetRecordsCalled:               	  metrics.GetOrRegisterCounter(MetricsSent, r),
		GetRecordsProvisionedThroughputExceeded:  metrics.GetOrRegisterCounter(MetricsGetRecordsProvisionedThroughputExceeded, r),
		GetRecordsTimeout:              	  metrics.GetOrRegisterCounter(MetricsGetRecordsTimeout, r),
		GetRecordsReadTimeout:          	  metrics.GetOrRegisterCounter(MetricsGetRecordsReadTimeout, r),
		ProcessedDuration:              	  metrics.GetOrRegisterGauge(MetricsProcessedDuration, r),
		GetRecordsDuration:             	  metrics.GetOrRegisterGauge(MetricsGetRecordsDuration, r),
		GetRecordsReadResponseDuration: 	  metrics.GetOrRegisterGauge(MetricsGetRecordsReadResponseDuration, r),
		GetRecordsUnmarshalDuration:    	  metrics.GetOrRegisterGauge(MetricsGetRecordsUnmarshalDuration, r),
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

// AddBatchSize records a count of the number of messages returned by
// GetRecords in the consumer.
func (dsc *DefaultConsumerStatsCollector) AddBatchSize(count int) {
	dsc.BatchSize.Inc(int64(count))
}

// AddGetRecordsCalled records the number of times the GetRecords API was called
// by the consumer.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsCalled(count int) {
	dsc.GetRecordsCalled.Inc(int64(count))
}

// AddProvisionedThroughputExceeded records the number of times the GetRecords
// API returned a ErrCodeProvisionedThroughputExceededException by the consumer.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsProvisionedThroughputExceeded(count int) {
	dsc.GetRecordsProvisionedThroughputExceeded.Inc(int64(count))
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

// AddProcessedDuration records the duration to process a record.  See notes on
// AddProcessed.
func (dsc *DefaultConsumerStatsCollector) AddProcessedDuration(duration time.Duration) {
	dsc.ProcessedDuration.Update(duration.Nanoseconds())
}

// AddGetRecordsDuration records the duration that the GetRecords API request
// took.  Only the times of successful calls are measured.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsDuration(duration time.Duration) {
	dsc.GetRecordsDuration.Update(duration.Nanoseconds())
}

// AddGetRecordsReadResponseDuration records the duration that it took to read
// the response body of a GetRecords API request.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsReadResponseDuration(duration time.Duration) {
	dsc.GetRecordsReadResponseDuration.Update(duration.Nanoseconds())
}

// AddGetRecordsUnmarshalDuration records the duration that it took to unmarshal
// the response body of a GetRecords API request.
func (dsc *DefaultConsumerStatsCollector) AddGetRecordsUnmarshalDuration(duration time.Duration) {
	dsc.GetRecordsUnmarshalDuration.Update(duration.Nanoseconds())
}

// PrintStats logs the stats
func (dsc *DefaultConsumerStatsCollector) PrintStats() {
	log.Printf("Listener stats: Consumed: [%d]\n", dsc.Consumed.Count())
	log.Printf("Listener stats: Delivered: [%d]\n", dsc.Delivered.Count())
	log.Printf("Listener stats: Processed: [%d]\n", dsc.Processed.Count())
	log.Printf("Listener stats: Batch Size: [%d]\n", dsc.BatchSize.Count())
	log.Printf("Listener stats: GetRecords Called: [%d]\n", dsc.GetRecordsCalled.Count())
	log.Printf("Listener stats: GetRecords Timeout: [%d]\n", dsc.GetRecordsTimeout.Count())
	log.Printf("Listener stats: GetRecords Read Timeout: [%d]\n", dsc.GetRecordsReadTimeout.Count())
	log.Printf("Listener stats: GetRecords Provisioned Throughput Exceeded: [%d]\n", dsc.GetRecordsProvisionedThroughputExceeded.Count())
	log.Printf("Listener stats: Processed Duration (ns): [%d]\n", dsc.ProcessedDuration.Value())
	log.Printf("Listener stats: GetRecords Duration (ns): [%d]\n", dsc.GetRecordsDuration.Value())
	log.Printf("Listener stats: GetRecords Read Response Duration (ns): [%d]\n", dsc.GetRecordsReadResponseDuration.Value())
	log.Printf("Listener stats: GetRecords Unmarshal Duration (ns): [%d]\n", dsc.GetRecordsUnmarshalDuration.Value())
}
