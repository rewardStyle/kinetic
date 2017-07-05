package listener

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic/logging"
)

// Config is used to configure a Listener instance
type Config struct {
	*listenerOptions
	AwsConfig *aws.Config
	LogLevel aws.LogLevelType
}

// NewConfig creates a new instance of Config
func NewConfig(cfg *aws.Config) *Config {
	return &Config{
		AwsConfig: cfg,
		listenerOptions: &listenerOptions{
			queueDepth:  10000,
			concurrency: 10000,
			Stats:       &NilStatsCollector{},
		},
		LogLevel: logging.LogOff,
	}
}

// SetQueueDepth controls the depth of the listener queue
func (c *Config) SetQueueDepth(queueDepth int) {
	c.queueDepth = queueDepth
}

// SetConcurrency controls the number of goroutines the Listener will spawn to
// process messages.
func (c *Config) SetConcurrency(concurrency int) {
	c.concurrency = concurrency
}

// SetStatsCollector configures a listener to handle listener metrics.
func (c *Config) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// SetLogLevel configures both the SDK and Kinetic log levels.
func (c *Config) SetLogLevel(logLevel aws.LogLevelType) {
	c.LogLevel = logLevel & 0xffff0000
}
