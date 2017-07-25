package producer

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic/errs"
)

const (
	kinesisMsgCountRateLimit = 1000    // AWS Kinesis limit of 1000 records/sec
	kinesisMsgSizeRateLimit  = 1000000 // AWS Kinesis limit of 1 MB/sec
)

// KinesisWriterConfig is used to configure KinesisWriter
type KinesisWriterConfig struct {
	*kinesisWriterOptions
	AwsConfig *aws.Config
	LogLevel  aws.LogLevelType
}

// NewKinesisWriterConfig creates a new instance of KinesisWriterConfig
func NewKinesisWriterConfig(cfg *aws.Config) *KinesisWriterConfig {
	return &KinesisWriterConfig{
		AwsConfig: cfg,
		kinesisWriterOptions: &kinesisWriterOptions{
			responseReadTimeout: time.Second,
			msgCountRateLimit:   kinesisMsgCountRateLimit,
			msgSizeRateLimit:    kinesisMsgSizeRateLimit,
			Stats:               &NilStatsCollector{},
		},
		LogLevel: *cfg.LogLevel,
	}
}

// SetResponseReadTimeout configures the time to wait for each successive Read operation on the GetRecords response payload.
func (c *KinesisWriterConfig) SetResponseReadTimeout(timeout time.Duration) {
	c.responseReadTimeout = timeout
}

// SetMsgCountRateLimit configures the maximum number of messages that can be sent per second
func (c *KinesisWriterConfig) SetMsgCountRateLimit(limit int) {
	if limit > 0 && limit <= kinesisMsgCountRateLimit {
		c.msgCountRateLimit = limit
	} else {
		log.Fatal("Message Count Rate Limit must be positive and less than ", kinesisMsgCountRateLimit)
		panic(errs.ErrInvalidMsgCountRateLimit)
	}
}

// SetMsgSizeRateLimit configures the maximum transmission size of the messages that can be sent per second
func (c *KinesisWriterConfig) SetMsgSizeRateLimit(limit int) {
	if limit > 0 && limit <= kinesisMsgSizeRateLimit {
		c.msgSizeRateLimit = limit
	} else {
		log.Fatal("Message Count Size Limit must be positive and less than ", kinesisMsgSizeRateLimit)
		panic(errs.ErrInvalidMsgSizeRateLimit)
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
