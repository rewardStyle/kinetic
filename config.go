package kinetic

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

// Config is used to configure a Kinetic instance
type Config struct {
	awsConfig *aws.Config
	*kineticConfig
}

// NewConfig creates a new instance of Config
func NewConfig() *Config {
	return &Config{
		awsConfig: aws.NewConfig().WithHTTPClient(&http.Client{
			Timeout: 5 * time.Minute,
		}),
		kineticConfig: &kineticConfig{
			logLevel: aws.LogOff,
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
