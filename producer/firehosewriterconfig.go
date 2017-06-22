package producer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic/config"
)

// FirehoseWriterConfig is used to configure FirehoseWriter
type FirehoseWriterConfig struct {
	*config.AwsOptions
	*firehoseWriterOptions
	LogLevel aws.LogLevelType
}

// NewFirehoseWriterConfig creates a new instance of FirehoseWriterConfig
func NewFirehoseWriterConfig(cfg *aws.Config) *FirehoseWriterConfig {
	return &FirehoseWriterConfig{
		AwsOptions: config.NewAwsOptionsFromConfig(cfg),
		firehoseWriterOptions: &firehoseWriterOptions{
			Stats: &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetStatsCollector configures a listener to handle listener metrics.
func (c *FirehoseWriterConfig) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// SetLogLevel configures the log levels for the SDK.
func (c *FirehoseWriterConfig) SetLogLevel(logLevel aws.LogLevelType) {
	c.LogLevel = logLevel & 0xffff0000
}
