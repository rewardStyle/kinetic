package config

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

// AwsOptions helps configure an aws.Config and session.Session
type AwsOptions struct {
	AwsConfig *aws.Config
}

// DefaultAwsOptions initializes the default aws.Config struct
func DefaultAwsOptions() *AwsOptions {
	return &AwsOptions{
		AwsConfig: aws.NewConfig().WithHTTPClient(
			&http.Client{
				Timeout: 10 * time.Minute,
			},
		),
	}
}

// SetCredentials configures AWS credentials.
func (c *AwsOptions) SetCredentials(accessKey, secretKey, sessionToken string) {
	c.AwsConfig.WithCredentials(
		credentials.NewStaticCredentials(accessKey, secretKey, sessionToken),
	)
}

// SetRegion configures the AWS region.
func (c *AwsOptions) SetRegion(region string) {
	c.AwsConfig.WithRegion(region)
}

// SetEndpoint sets the endpoint to be used by aws-sdk-go.
func (c *AwsOptions) SetEndpoint(endpoint string) {
	c.AwsConfig.WithEndpoint(endpoint)
}

// SetLogger configures the logger for Kinetic and the aws-sdk-go.
func (c *AwsOptions) SetLogger(logger aws.Logger) {
	c.AwsConfig.WithLogger(logger)
}

// SetLogLevel configures the log levels for the SDK.
func (c *AwsOptions) SetLogLevel(logLevel aws.LogLevelType) {
	c.AwsConfig.WithLogLevel(logLevel & 0xffff)
}

// SetHTTPClientTimeout configures the HTTP timeout for the SDK.
func (c *AwsOptions) SetHTTPClientTimeout(timeout time.Duration) {
	c.AwsConfig.WithHTTPClient(&http.Client{
		Timeout: timeout,
	})
}

// GetSession creates an instance of the session.Session to be used when creating service
// clients in aws-sdk-go.
func (c *AwsOptions) GetSession() (*session.Session, error) {
	return session.NewSession(c.AwsConfig)
}
