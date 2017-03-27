package listener

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

const ()

type Config struct {
	awsConfig *aws.Config

	stream string

	logLevel aws.LogLevelType
	stats    StatsListener

	getRecordsReadTimeout time.Duration

	shard         string
	batchSize     int
	concurrency   int
	shardIterator *ShardIterator
}

func NewConfig(stream string) *Config {
	return &Config{
		awsConfig: aws.NewConfig().WithHTTPClient(&http.Client{
			Timeout: 5 * time.Minute,
		}),
		stream:                stream,
		logLevel:              aws.LogOff,
		stats:                 &NilStatsListener{},
		batchSize:             10000,
		concurrency:           10000,
		shardIterator:         NewShardIterator(),
		getRecordsReadTimeout: 1 * time.Second,
	}
}

func (c *Config) WithCredentials(accessKey, secretKey, securityToken string) *Config {
	c.awsConfig.WithCredentials(
		credentials.NewStaticCredentials(accessKey, secretKey, securityToken),
	)
	return c
}

func (c *Config) WithRegion(region string) *Config {
	c.awsConfig.WithRegion(region)
	return c
}

func (c *Config) WithEndpoint(endpoint string) *Config {
	c.awsConfig.WithEndpoint(endpoint)
	return c
}

func (c *Config) WithLogger(logger aws.Logger) *Config {
	c.awsConfig.WithLogger(logger)
	return c
}

func (c *Config) WithLogLevel(logLevel aws.LogLevelType) *Config {
	c.awsConfig.WithLogLevel(logLevel & 0xffff)
	c.logLevel = logLevel >> 16
	return c
}

func (c *Config) WithHttpClientTimeout(timeout time.Duration) *Config {
	c.awsConfig.WithHTTPClient(&http.Client{
		Timeout: timeout,
	})
	return c
}

func (c *Config) WithStatsListener(stats StatsListener) *Config {
	c.stats = stats
	return c
}

func (c *Config) WithShardId(shard string) *Config {
	c.shard = shard
	return c
}

func (c *Config) WithBatchSize(batchSize int) *Config {
	c.batchSize = batchSize
	return c
}

func (c *Config) WithConcurrency(concurrency int) *Config {
	c.concurrency = concurrency
	return c
}

func (c *Config) WithInitialShardIterator(shardIterator *ShardIterator) *Config {
	c.shardIterator = shardIterator
	return c
}

func (c *Config) WithGetRecordsReadTimeout(timouet time.Duration) *Config {
	c.getRecordsReadTimeout = timouet
	return c
}

func (c *Config) GetAwsSession() (*session.Session, error) {
	return session.NewSession(c.awsConfig)
}
