package listener

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/logging"
)

// Config is used to configure a Listener instance
type Config struct {
	*kinetic.AwsOptions
	*listenerOptions
}

// NewConfig creates a new instance of Config
func NewConfig(stream, shard string) *Config {
	return &Config{
		AwsOptions: kinetic.DefaultAwsOptions(),
		listenerOptions: &listenerOptions{
			stream:                stream,
			shard:                 shard,
			batchSize:             10000,
			concurrency:           10000,
			shardIterator:         NewShardIterator(),
			getRecordsReadTimeout: 1 * time.Second,
			stats:    &NilStatsListener{},
			logLevel: logging.LogOff,
		},
	}
}

// SetLogLevel configures both the SDK and Kinetic log levels.
func (c *Config) SetLogLevel(logLevel aws.LogLevelType) {
	c.AwsOptions.SetLogLevel(logLevel)
	c.listenerOptions.logLevel = logLevel & 0xffff0000
}

// SetBatchSize configures the batch size of the GetRecords call.
func (c *Config) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

// SetConcurrency controls the number of goroutines the Listener will spawn to
// process messages.
func (c *Config) SetConcurrency(concurrency int) {
	c.concurrency = concurrency
}

// SetInitialShardIterator configures the settings used to retrieve initial
// shard iterator via the GetShardIterator call.
func (c *Config) SetInitialShardIterator(shardIterator *ShardIterator) {
	c.shardIterator = shardIterator
}

// SetGetRecordsReadTimeout configures the time to wait for each successive
// Read operation on the GetRecords response payload.
func (c *Config) SetGetRecordsReadTimeout(timouet time.Duration) {
	c.getRecordsReadTimeout = timouet
}

// SetStatsListener configures a listener to handle metrics.
func (c *Config) SetStatsListener(stats StatsListener) {
	c.stats = stats
}

// FromKinetic configures the session from Kinetic.
func (c *Config) FromKinetic(k *kinetic.Kinetic) *Config {
	c.AwsConfig = k.GetSession().Config
	return c
}

// GetSession creates an instance of the session.Session to be used when creating service
// clients in aws-sdk-go.
func (c *Config) GetSession() (*session.Session, error) {
	return session.NewSession(c.AwsConfig)
}
