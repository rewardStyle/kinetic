package listener

import (
	"time"
	"github.com/aws/aws-sdk-go/aws"
)

// KinesisReaderConfig is used to configure a KinesisReader
type KinesisReaderConfig struct {
	*kinesisReaderOptions
	AwsConfig *aws.Config
	LogLevel aws.LogLevelType
}

// NewKinesisReaderConfig creates a new instance of KinesisReaderConfig
func NewKinesisReaderConfig(cfg *aws.Config) *KinesisReaderConfig {
	return &KinesisReaderConfig{
		AwsConfig: cfg,
		kinesisReaderOptions: &kinesisReaderOptions{
			batchSize:     10000,
			shardIterator: NewShardIterator(),
			readTimeout:   time.Second,
			Stats:         &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetBatchSize configures the batch size of the GetRecords call.
func (c *KinesisReaderConfig) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

// SetInitialShardIterator configures the settings used to retrieve initial shard iterator via the GetShardIterator
// call.
func (c *KinesisReaderConfig) SetInitialShardIterator(shardIterator *ShardIterator) {
	c.shardIterator = shardIterator
}

// SetReadTimeout configures the time to wait for each successive Read operation on the GetRecords response payload.
func (c *KinesisReaderConfig) SetReadTimeout(timeout time.Duration) {
	c.readTimeout = timeout
}

// SetStatsCollector configures a listener to handle listener metrics.
func (c *KinesisReaderConfig) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// SetLogLevel configures the log levels for the SDK.
func (c *KinesisReaderConfig) SetLogLevel(logLevel aws.LogLevelType) {
	c.LogLevel = logLevel & 0xffff0000
}
