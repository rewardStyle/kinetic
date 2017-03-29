package logging

import (
	"github.com/aws/aws-sdk-go/aws"
)

const (
	LogOff aws.LogLevelType = (iota * 0x1000) << 16
	LogDebug
)

const (
	LogPlaceholder aws.LogLevelType = LogDebug | (1 << (16 + iota))
)
