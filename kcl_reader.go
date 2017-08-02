package kinetic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
)

const (
	kclReaderMaxBatchSize = 10000
)

// kclReaderOptions is a struct that holds all of the KclReader's configurable parameters.
type kclReaderOptions struct {
	batchSize              int                    // maximum records per GetRecordsRequest call // callback function that gets called after shutdown
	logLevel               aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats                  ConsumerStatsCollector // stats collection mechanism
}

// defaultkclReaderOptions instantiates a kclReaderOptions with default values.
func defaultKclReaderOptions() *kclReaderOptions {
	return &kclReaderOptions{
		batchSize:              kclReaderMaxBatchSize,
		logLevel:               aws.LogOff,
		Stats:                  &NilConsumerStatsCollector{},
	}
}

// kclReaderOptionsFn is a method signature for defining functional option methods for configuring the KclReader.
type kclReaderOptionsFn func(*kclReaderOptions) error

// kclReaderBatchSize is a functional option method for configuring the KclReader's batch size
func kclReaderBatchSize(size int) kclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		if size >= 0 && size <= kclReaderMaxBatchSize {
			o.batchSize = size
			return nil
		}
		return ErrInvalidBatchSize
	}
}

// kclReaderLogLevel is a functional option method for configuring the KclReader's log level.
func kclReaderLogLevel(ll aws.LogLevelType) kclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.logLevel = ll
		return nil
	}
}

// kclReaderStats is a functional option method for configuring the KclReader's stats collector.
func kclReaderStats(sc ConsumerStatsCollector) kclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.Stats = sc
		return nil
	}
}

// KclReader handles the KCL Multilang Protocol to read records from KCL
type KclReader struct {
	*kclReaderOptions
	*LogHelper
	pipeOfDeath chan empty
	stop        chan empty
	scanner     *bufio.Scanner
	reader      *bufio.Reader
	messages    chan *Message
}

// NewKclReader creates a new stream reader to read records from KCL
func NewKclReader(c *aws.Config, optionFns ...kclReaderOptionsFn) (*KclReader, error) {
	kclReaderOptions := defaultKclReaderOptions()
	for _, optionFn := range optionFns {
		optionFn(kclReaderOptions)
	}
	return &KclReader{
		messages: make(chan *Message),
		kclReaderOptions: kclReaderOptions,
		LogHelper: &LogHelper{
			LogLevel: kclReaderOptions.logLevel,
			Logger:   c.Logger,
		},
	}, nil
}

// processRecords is a helper method which pulls from the reader's message channel and calls the callback function
func (r *KclReader) processRecords(batchSize int, fn messageHandler) (count int, size int, err error) {
	for i := 0; i < batchSize; i++ {
		msg := <-r.messages
		err = fn(msg)
		if err != nil {
			r.LogError("messageHandler resulted in an error: ", err)
		} else {
			r.Stats.AddConsumed(1)
			count++
			b, err := json.Marshal(msg)
			if err != nil {
				r.LogError("Unable to marshal message: ", err)
			} else {
				size += len(b)
			}
		}
	}

	return count, size, nil
}

func (r *KclReader) getAction() (*actionMessage, error) {
	buffer := &bytes.Buffer{}
	for {
		line, isPrefix, err := r.reader.ReadLine()
		if err != nil {
			panic("Unable to read line from stdin " + err.Error())
		}
		buffer.Write(line)
		if !isPrefix {
			break
		}
	}

	actionMsg := &actionMessage{}
	err := json.Unmarshal(buffer.Bytes(), actionMsg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not understand line read from input: %s\n", buffer.String())
	}
	return actionMsg, nil
}

// processAction listens to STDIN and processes action messages based on the Multilang protocol from KCL
func (r *KclReader) processAction() error {
	for {
		// Retrieve action message
		actionMessage, err := r.getAction()
		if err != nil {
			return err
		}
		if actionMessage == nil {
			break
		}

		switch actionMessage.Action {
		case kclActionTypeInitialize:
			err := r.sendMessage(newStatusMessage(kclActionTypeInitialize))
			if err != nil {
				r.LogError("Unable to send Initialize acknowledgement due to: ", err)
			}
		case kclActionTypeCheckpoint:
			err := r.sendMessage(newStatusMessage(kclActionTypeCheckpoint))
			if err != nil {
				r.LogError("Unable to send Checkpoint acknowledgement due to: ", err)
			}
		case kcActionTypeShutdown:
			err := r.sendMessage(newStatusMessage(kcActionTypeShutdown))
			if err != nil {
				r.LogError("Unable to send Shutdown acknowledgement due to: ", err)
			}
		case kclActionTypeProcessRecords:
			// Put all the messages on the reader's message channel
			for _, msg := range actionMessage.Records {
				r.messages <-msg.ToMessage()
			}

			// Send an acknowledgement that all the messages were received
			err := r.sendMessage(newStatusMessage(kclActionTypeProcessRecords))
			if err != nil {
				r.LogError("Unable to send ProcessRecords acknowledgement due to: ", err)
			}
		default:
			r.LogError("processAction received an invalid action: ", actionMessage.Action)
		}
	}

	return nil
}

// sendMessage
func (r *KclReader) sendMessage(msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		r.LogError(err)
		return err
	}

	fmt.Fprintln(os.Stdout, string(b))

	return nil
}

// GetRecord calls processRecords to attempt to put one message from message buffer to the consumer's message
// channel
func (r *KclReader) GetRecord(ctx context.Context, fn messageHandler) (count int, size int, err error) {
	count, size, err = r.processRecords(1, fn)
	return count, size, err
}

// GetRecords calls processRecords to attempt to put all messages on the message buffer on the consumer's
// message channel
func (r *KclReader) GetRecords(ctx context.Context, fn messageHandler) (count int, size int, err error) {
	count, size, err = r.processRecords(r.batchSize, fn)
	return count, size, err
}
