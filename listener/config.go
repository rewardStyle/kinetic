package listener

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/logging"
)

type Config struct {
	awsConfig *aws.Config
	*listenerConfig
}

func NewConfig(stream, shard string) *Config {
	return &Config{
		awsConfig: aws.NewConfig().WithHTTPClient(&http.Client{
			Timeout: 5 * time.Minute,
		}),
		listenerConfig: &listenerConfig{
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
	c.logLevel = logLevel & 0xffff0000
	return c
}

func (c *Config) WithHttpClientTimeout(timeout time.Duration) *Config {
	c.awsConfig.WithHTTPClient(&http.Client{
		Timeout: timeout,
	})
	return c
}

func (c *Config) GetSession() (*session.Session, error) {
	return session.NewSession(c.awsConfig)
}

func (c *Config) FromKinetic(k *kinetic.Kinetic) *Config {
	c.awsConfig = k.GetSession().Config
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

func (c *Config) WithStatsListener(stats StatsListener) *Config {
	c.stats = stats
	return c
}
