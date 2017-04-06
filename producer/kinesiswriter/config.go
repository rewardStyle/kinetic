package kinesiswriter

import ()

// Config is used to configure a Kinesis Writer instance.
type Config struct {
	*kinesisWriterOptions
}

// NewConfig creates a new instance of Config.
func NewConfig() *Config {
	return &Config{
		kinesisWriterOptions: &kinesisWriterOptions{},
	}
}
