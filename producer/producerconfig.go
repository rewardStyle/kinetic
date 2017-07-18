package producer

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic/errs"
)

// Config is used to configure a Producer instance.
type Config struct {
	*producerOptions
	AwsConfig *aws.Config
	LogLevel  aws.LogLevelType
}

// NewConfig creates a new instance of Config.
func NewConfig(cfg *aws.Config) *Config {
	return &Config{
		AwsConfig: cfg,
		producerOptions: &producerOptions{
			batchSize:        500,
			batchTimeout:     time.Second,
			queueDepth:       500,
			maxRetryAttempts: 10,
			workersPerShard:  5,
			shardCount:       1,
			rateLimit:        1000,
			resetFrequency:   time.Second,
			Stats:            &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetBatchSize configures the batch size to flush pending records to the PutRecords call.
func (c *Config) SetBatchSize(batchSize int) {
	if batchSize > 0 && batchSize <= 500 {
		c.batchSize = batchSize
	} else {
		// http://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html
		log.Fatal("BatchSize must be less than or equal to 500 ")
		panic(errs.ErrInvalidBatchSize)
	}
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

// SetWorkersPerShard defines the number of concurrent workers to run per active shard
func (c *Config) SetWorkersPerShard(count int) {
	c.workersPerShard = count
}

// SetShardCount defines the initial shard size
func (c *Config) SetShardCount(count int) {
	c.shardCount = count
}

// SetRateLimit defines the maximum number of message to send per cycle
func (c *Config) SetRateLimit(limit int) {
	c.rateLimit = limit
}

// SetResetFrequency defines the frequency at which the rateLimiter resets
func (c *Config) SetResetFrequency(freq time.Duration) {
	c.resetFrequency = freq
}

// SetStatsCollector configures a listener to handle producer metrics.
func (c *Config) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// SetLogLevel configures both the SDK and Kinetic log levels.
func (c *Config) SetLogLevel(logLevel aws.LogLevelType) {
	c.LogLevel = logLevel & 0xffff0000
}
