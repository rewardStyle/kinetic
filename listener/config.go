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

// Config is used to configure a Listener instance
type Config struct {
	awsConfig *aws.Config
	*listenerConfig
}

// NewConfig creates a new instance of Config
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

// WithCredentials configures AWS credentials.  Leave blank to use environment
// variables, IAM roles or ~/.aws/credentials.  See aws-sdk-go docs for more
// details.
func (c *Config) WithCredentials(accessKey, secretKey, securityToken string) *Config {
	c.awsConfig.WithCredentials(
		credentials.NewStaticCredentials(accessKey, secretKey, securityToken),
	)
	return c
}

// WithRegion configures the AWS region.  Leave blank to use environment
// variables.  See aws-sdk-go for more details.
func (c *Config) WithRegion(region string) *Config {
	c.awsConfig.WithRegion(region)
	return c
}

// WithEndpoint sets the endpoint to be used by aws-sdk-go.
func (c *Config) WithEndpoint(endpoint string) *Config {
	c.awsConfig.WithEndpoint(endpoint)
	return c
}

// WithLogger configures the logger for Kinetic and the aws-sdk-go.
func (c *Config) WithLogger(logger aws.Logger) *Config {
	c.awsConfig.WithLogger(logger)
	return c
}

// WithLogLevel configures the log levels for Kinetic and the aws-sdk-go.  Note
// that log levels for the SDK can be found in aws-sdk-go/aws package.  Kinetic
// log levels are found in the kinetic/logging package.
func (c *Config) WithLogLevel(logLevel aws.LogLevelType) *Config {
	c.awsConfig.WithLogLevel(logLevel & 0xffff)
	c.logLevel = logLevel & 0xffff0000
	return c
}

// WithHTTPClientTimeout configures the HTTP timeout for the SDK.
func (c *Config) WithHTTPClientTimeout(timeout time.Duration) *Config {
	c.awsConfig.WithHTTPClient(&http.Client{
		Timeout: timeout,
	})
	return c
}

// GetSession creates an instance of the session.Session to be used when creating service
// clients in aws-sdk-go.
func (c *Config) GetSession() (*session.Session, error) {
	return session.NewSession(c.awsConfig)
}

// FromKinetic configures the session from Kinetic.
func (c *Config) FromKinetic(k *kinetic.Kinetic) *Config {
	c.awsConfig = k.GetSession().Config
	return c
}

// WithBatchSize configures the batch size of the GetRecords call.
func (c *Config) WithBatchSize(batchSize int) *Config {
	c.batchSize = batchSize
	return c
}

// WithConcurrency controls the number of goroutines the Listener will spawn to
// process messages.
func (c *Config) WithConcurrency(concurrency int) *Config {
	c.concurrency = concurrency
	return c
}

// WithInitialShardIterator configures the settings used to retrieve initial
// shard iterator via the GetShardIterator call.
func (c *Config) WithInitialShardIterator(shardIterator *ShardIterator) *Config {
	c.shardIterator = shardIterator
	return c
}

// WithGetRecordsReadTimeout configures the time to wait for each successive
// Read operation on the GetRecords response payload.
func (c *Config) WithGetRecordsReadTimeout(timouet time.Duration) *Config {
	c.getRecordsReadTimeout = timouet
	return c
}

// WithStatsListener configures a listener to handle metrics.
func (c *Config) WithStatsListener(stats StatsListener) *Config {
	c.stats = stats
	return c
}
