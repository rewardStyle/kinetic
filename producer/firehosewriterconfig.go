package producer

import (
	"github.com/aws/aws-sdk-go/aws"
)

const (
	firehoseMsgCountRateLimit = 5000    // AWS Firehose limit of 5000 records/sec
	firehoseMsgSizeRateLimit  = 5000000 // AWS Firehose limit of 5 MB/sec
)

// FirehoseWriterConfig is used to configure FirehoseWriter
type FirehoseWriterConfig struct {
	*firehoseWriterOptions
	AwsConfig *aws.Config
	LogLevel  aws.LogLevelType
}

// NewFirehoseWriterConfig creates a new instance of FirehoseWriterConfig
func NewFirehoseWriterConfig(cfg *aws.Config) *FirehoseWriterConfig {
	return &FirehoseWriterConfig{
		AwsConfig: cfg,
		firehoseWriterOptions: &firehoseWriterOptions{
			msgCountRateLimit: firehoseMsgCountRateLimit,
			msgSizeRateLimit:  firehoseMsgSizeRateLimit,
			Stats:             &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetMsgCountRateLimit configures the maximum number of messages that can be sent per second
func (c *FirehoseWriterConfig) SetMsgCountRateLimit(limit int) {
	if limit > firehoseMsgCountRateLimit {

	}
	c.msgCountRateLimit = limit
}

// SetMsgSizeRateLimit configures the maximum transmission size of the messages that can be sent per second
func (c *FirehoseWriterConfig) SetMsgSizeRateLimit(limit int) {
	if limit > firehoseMsgSizeRateLimit {

	}
	c.msgSizeRateLimit = limit
}

// SetStatsCollector configures a listener to handle listener metrics.
func (c *FirehoseWriterConfig) SetStatsCollector(stats StatsCollector) {
	c.Stats = stats
}

// SetLogLevel configures the log levels for the SDK.
func (c *FirehoseWriterConfig) SetLogLevel(logLevel aws.LogLevelType) {
	c.LogLevel = logLevel & 0xffff0000
}
