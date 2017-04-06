package producer

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/logging"
)

// Config is used to configure a Producer instance.
type Config struct {
	*kinetic.AwsOptions
	*producerOptions
}

// NewConfig creates a new instance of Config.
func NewConfig() *Config {
	return &Config{
		AwsOptions: kinetic.DefaultAwsOptions(),
		producerOptions: &producerOptions{
			batchSize:    500,
			batchTimeout: 1 * time.Second,
			concurrency:  1,
			queueDepth:   500,
			LogLevel:     logging.LogOff,
			Stats:        &NilStatsCollector{},
		},
	}
}

// SetBatchSize configures the batch size to flush pending records to the
// PutRecords call.
func (c *Config) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

// SetBatchTimeout configures the timeout to flush pending records to the
// PutRecords call.
func (c *Config) SetBatchTimeout(timeout time.Duration) {
	c.batchTimeout = timeout
}

// SetConcurrency controls the number of outstanding PutRecords calls may be
// active at a time.
func (c *Config) SetConcurrency(concurrency int) {
	c.concurrency = concurrency
}

// SetQueueDepth controls the number of messages that can be in the channel
// to be processed by produce at a given time.
func (c *Config) SetQueueDepth(queueDepth int) {
	c.queueDepth = queueDepth
}

// SetWriter sets the underlying stream writer (Kinesis or Firehose) for the
// producer.  There can only be a single writer associated with a producer (as
// various retry logic/state is not easily shared between multiple writers).  If
// multiple streams are desired, create two different producers and write to
// both.
func (c *Config) SetWriter(writer StreamWriter) {
	c.writer = writer
}

// SetLogLevel configures both the SDK and Kinetic log levels.
func (c *Config) SetLogLevel(logLevel aws.LogLevelType) {
	c.AwsOptions.SetLogLevel(logLevel)
	c.LogLevel = logLevel & 0xffff0000
}

// SetStatsCollector configures a listener to handle producer metrics.
func (c *Config) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// FromKinetic configures the session from Kinetic.
func (c *Config) FromKinetic(k *kinetic.Kinetic) *Config {
	c.AwsConfig = k.Session.Config
	return c
}
