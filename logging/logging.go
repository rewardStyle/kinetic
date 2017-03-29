package logging

import (
	"github.com/aws/aws-sdk-go/aws"
)

const (
	// LogOff disables all logging.
	LogOff aws.LogLevelType = (iota * 0x1000) << 16

	// LogDebug enables debug logging.
	LogDebug
)

const (
	// LogPlaceholder is a placeholder debug sub-level.
	LogPlaceholder aws.LogLevelType = LogDebug | (1 << (16 + iota))
)
