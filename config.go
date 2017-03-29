package kinetic

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

type Config struct {
	awsConfig *aws.Config
	*kineticConfig
}

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
