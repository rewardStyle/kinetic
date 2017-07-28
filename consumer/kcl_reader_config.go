package consumer

import (
	"github.com/aws/aws-sdk-go/aws"
)

// KclReaderConfig is used to configure KclReader
type KclReaderConfig struct {
	*kclReaderOptions
	AwsConfig *aws.Config
	LogLevel  aws.LogLevelType
}

// NewKclReaderConfig creates a new instance of KclReaderConfig
func NewKclReaderConfig(cfg *aws.Config) *KclReaderConfig {
	return &KclReaderConfig{
		AwsConfig: cfg,
		kclReaderOptions: &kclReaderOptions{
			stats: &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetOnInitCallbackFn configures a callback function which is run prior to sending a status message
// acknowledging an 'initialize' message was received / processed
func (c *KclReaderConfig) SetOnInitCallbackFn(fn func() error) {
	c.onInitCallbackFn = fn
}

// SetOnCheckpointCallbackFn configures a callback function which is run prior to sending a status message
// acknowledging an 'checkpoint' message was received / processed
func (c *KclReaderConfig) SetOnCheckpointCallbackFn(fn func() error) {
	c.onCheckpointCallbackFn = fn
}

// SetOnShutdownCallbackFn configures a callback function which is run prior to sending a status message
// acknowledging a 'shutdown' message was received / processed
func (c *KclReaderConfig) SetOnShutdownCallbackFn(fn func() error) {
	c.onShutdownCallbackFn = fn
}

// SetStatsCollector configures a listener to handle listener metrics.
func (c *KclReaderConfig) SetStatsCollector(stats StatsCollector) {
	c.stats = stats
}

// SetLogLevel configures both the SDK and Kinetic log levels.
func (c *KclReaderConfig) SetLogLevel(logLevel aws.LogLevelType) {
	c.LogLevel = logLevel & 0xffff0000
}
