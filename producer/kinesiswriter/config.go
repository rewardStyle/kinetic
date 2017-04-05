package kinesiswriter

import ()

type Config struct {
	*kinesisWriterOptions
}

func NewConfig() *Config {
	return &Config{
		kinesisWriterOptions: &kinesisWriterOptions{},
	}
}
