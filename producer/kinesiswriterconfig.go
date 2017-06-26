package producer

import (
	"github.com/aws/aws-sdk-go/aws"
)

// KinesisWriterConfig is used to configure KinesisWriter
type KinesisWriterConfig struct {
	*kinesisWriterOptions
	AwsConfig *aws.Config
	LogLevel aws.LogLevelType
}

// NewKinesisWriterConfig creates a new instance of KinesisWriterConfig
func NewKinesisWriterConfig(cfg *aws.Config) *KinesisWriterConfig {
	return &KinesisWriterConfig{
		AwsConfig: cfg,
		kinesisWriterOptions: &kinesisWriterOptions{
			Stats: &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetStatsCollector configures a listener to handle listener metrics.
func (c *KinesisWriterConfig) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// SetLogLevel configures the log levels for the SDK.
func (c *KinesisWriterConfig) SetLogLevel(logLevel aws.LogLevelType) {
	c.LogLevel = logLevel & 0xffff0000
}
