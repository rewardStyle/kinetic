package producer

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic/config"
)

// Config is used to configure a Producer instance.
type Config struct {
	*config.AwsOptions
	*producerOptions
	LogLevel aws.LogLevelType
}

// NewConfig creates a new instance of Config.
func NewConfig(cfg *aws.Config) *Config {
	return &Config{
		AwsOptions: config.NewAwsOptionsFromConfig(cfg),
		producerOptions: &producerOptions{
			batchSize:        500,
			batchTimeout:     time.Second,
			queueDepth:       500,
			maxRetryAttempts: 10,
			concurrency:      1,
			Stats:            &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetBatchSize configures the batch size to flush pending records to the PutRecords call.
func (c *Config) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

// SetBatchTimeout configures the timeout to flush pending records to the PutRecords call.
func (c *Config) SetBatchTimeout(timeout time.Duration) {
	c.batchTimeout = timeout
}

// SetQueueDepth controls the number of messages that can be in the channel to be processed by produce at a given time.
func (c *Config) SetQueueDepth(queueDepth int) {
	c.queueDepth = queueDepth
}

// SetMaxRetryAttempts controls the number of times a message can be retried before it is discarded.
func (c *Config) SetMaxRetryAttempts(attempts int) {
	c.maxRetryAttempts = attempts
}

// SetConcurrency controls the number of outstanding PutRecords calls may be active at a time.
func (c *Config) SetConcurrency(concurrency int) {
	c.concurrency = concurrency
}

// SetStatsCollector configures a listener to handle producer metrics.
func (c *Config) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// SetLogLevel configures both the SDK and Kinetic log levels.
func (c *Config) SetLogLevel(logLevel aws.LogLevelType) {
	// TODO: Do we want to change the AWS log level?
	c.AwsOptions.SetLogLevel(logLevel)
	c.LogLevel = logLevel & 0xffff0000
}
