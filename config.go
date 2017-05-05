package kinetic

import (
	"github.com/aws/aws-sdk-go/aws"

	"github.com/rewardStyle/kinetic/config"
)

// Config is used to configure a Kinetic instance
type Config struct {
	*config.AwsOptions
	*kineticOptions
	LogLevel aws.LogLevelType
}

// NewConfig creates a new instance of Config
func NewConfig() *Config {
	return &Config{
		AwsOptions:     config.DefaultAwsOptions(),
		kineticOptions: &kineticOptions{},
		LogLevel:       aws.LogOff,
	}
}

// SetLogLevel configures both the SDK and Kinetic log levels.
func (c *Config) SetLogLevel(logLevel aws.LogLevelType) {
	c.AwsOptions.SetLogLevel(logLevel)
	c.LogLevel = logLevel & 0xffff0000
}
