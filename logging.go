package kinetic

import (
	"github.com/aws/aws-sdk-go/aws"
)

const (
	// LogOff disables all kinetic.
	LogOff aws.LogLevelType = (iota * 0x1000) << 16

	// LogBug enables logging of bugs in code.
	LogBug

	// LogError enables logging of errors.
	LogError

	// LogInfo enables logging of informational messages..
	LogInfo

	// LogDebug enables debug kinetic.
	LogDebug
)

const (
	// LogPlaceholder is a placeholder debug sub-level.
	LogPlaceholder aws.LogLevelType = LogDebug | (1 << (16 + iota))
)

// ILogHelper is an interface for LogHelper
type ILogHelper interface {
	Log(aws.LogLevelType, ...interface{})
	LogBug(...interface{})
	LogError(...interface{})
	LogInfo(...interface{})
	LogDebug(...interface{})
}

// LogHelper is used for defining log configuration
type LogHelper struct {
	LogLevel aws.LogLevelType
	Logger   aws.Logger
}

// Log handles levelled logging
func (l *LogHelper) Log(level aws.LogLevelType, args ...interface{}) {
	if l.LogLevel.Matches(level) {
		l.Logger.Log(args...)
	}
}

// LogBug logs a BUG in the code.
func (l *LogHelper) LogBug(args ...interface{}) {
	l.Log(LogBug, args...)
}

// LogError logs an error.
func (l *LogHelper) LogError(args ...interface{}) {
	l.Log(LogError, args...)
}

// LogInfo logs an informational kinetic.
func (l *LogHelper) LogInfo(args ...interface{}) {
	l.Log(LogInfo, args...)
}

// LogDebug a debug message using the AWS SDK logger.
func (l *LogHelper) LogDebug(args ...interface{}) {
	l.Log(LogDebug, args...)
}
