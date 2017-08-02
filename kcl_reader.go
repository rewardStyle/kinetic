package kinetic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
)

const (
	kclReaderMaxBatchSize = 10000
)

// kclReaderOptions is a struct that holds all of the KclReader's configurable parameters.
type kclReaderOptions struct {
	batchSize              int                    // maximum records per GetRecordsRequest call
	onInitCallbackFn       func() error           // callback function that gets called after initialization
	onCheckpointCallbackFn func() error           // callback function that gets called after checkpointing
	onShutdownCallbackFn   func() error           // callback function that gets called after shutdown
	logLevel               aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats                  ConsumerStatsCollector // stats collection mechanism
}

// defaultkclReaderOptions instantiates a kclReaderOptions with default values.
func defaultKclReaderOptions() *kclReaderOptions {
	return &kclReaderOptions{
		batchSize: kclReaderMaxBatchSize,
		logLevel:  aws.LogOff,
		Stats:     &NilConsumerStatsCollector{},
	}
}

// KclReaderOptionsFn is a method signature for defining functional option methods for configuring the KclReader.
type KclReaderOptionsFn func(*kclReaderOptions) error

// kclReaderBatchSize is a functional option method for configuring the KclReader's batch size
func kclReaderBatchSize(size int) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		if size > 0 && size <= kclReaderMaxBatchSize {
			o.batchSize = size
			return nil
		}
		return ErrInvalidBatchSize
	}
}

// KclReaderOnInitCallbackFn is a functional option method for configuring the KclReader's
// onInitCallbackFn.
func KclReaderOnInitCallbackFn(fn func() error) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.onInitCallbackFn = fn
		return nil
	}
}

// KclReaderOnCheckpointCallbackFn is a functional option method for configuring the KclReader's
// onCheckpointCallbackFn.
func KclReaderOnCheckpointCallbackFn(fn func() error) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.onCheckpointCallbackFn = fn
		return nil
	}
}

// KclReaderOnShutdownCallbackFn is a functional option method for configuring the KclReader's
// onShutdownCallbackFn.
func KclReaderOnShutdownCallbackFn(fn func() error) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.onShutdownCallbackFn = fn
		return nil
	}
}

// kclReaderLogLevel is a functional option method for configuring the KclReader's log level.
func kclReaderLogLevel(ll aws.LogLevelType) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.logLevel = ll
		return nil
	}
}

// kclReaderStats is a functional option method for configuring the KclReader's stats collector.
func kclReaderStats(sc ConsumerStatsCollector) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.Stats = sc
		return nil
	}
}

// KclReader handles the KCL Multilang Protocol to read records from KCL
type KclReader struct {
	*kclReaderOptions           // contains all of the configuration settings for the KclReader
	*LogHelper		    // object for help with logging
	reader       *bufio.Reader  // io reader to read from STDIN
	messages     chan *Message  // unbuffered message channel used to throttle the record processing from KCL
	startupOnce  sync.Once      // used to ensure that the startup function is called once
	shutdownOnce sync.Once      // used to ensure that the shutdown function is called once
}

// NewKclReader creates a new stream reader to read records from KCL
func NewKclReader(c *aws.Config, optionFns ...KclReaderOptionsFn) (*KclReader, error) {
	kclReaderOptions := defaultKclReaderOptions()
	for _, optionFn := range optionFns {
		optionFn(kclReaderOptions)
	}
	return &KclReader{
		kclReaderOptions: kclReaderOptions,
		LogHelper: &LogHelper{
			LogLevel: kclReaderOptions.logLevel,
			Logger:   c.Logger,
		},
	}, nil
}

func (r *KclReader) process(ctx context.Context) {
	r.startupOnce.Do(func (){
		defer func(){
			// Reset shutdownOnce to allow the shut down sequence to happen again
			r.shutdownOnce = sync.Once{}
		}()

		// create communication channels
		r.messages = make(chan *Message)

		// listen to STDIN and processes action messages based on the Multilang protocol from KCL
		go func(){
			defer r.shutdown()

			for {
				select {
				case <-ctx.Done():
					r.LogInfo("KclReader received ctx.Done() while processing messages from KCL")
					return
				default:
				}

				// Retrieve action message
				actionMessage, err := r.getAction()
				if err != nil || actionMessage == nil {
					return
				}

				switch actionMessage.Action {
				case kclActionTypeInitialize:
					r.LogDebug("Receieved Initialize action from KCL")
					r.onInitCallbackFn()
					err := r.sendMessage(newStatusMessage(kclActionTypeInitialize))
					if err != nil {
						r.LogError("Unable to send Initialize acknowledgement due to: ", err)
					}
				case kclActionTypeCheckpoint:
					r.LogDebug("Receieved Checkpoint action from KCL")
					r.onCheckpointCallbackFn()
					err := r.sendMessage(newStatusMessage(kclActionTypeCheckpoint))
					if err != nil {
						r.LogError("Unable to send Checkpoint acknowledgement due to: ", err)
					}
				case kcActionTypeShutdown:
					r.LogDebug("Receieved Shutdown action from KCL")
					r.onShutdownCallbackFn()
					err := r.sendMessage(newStatusMessage(kcActionTypeShutdown))
					if err != nil {
						r.LogError("Unable to send Shutdown acknowledgement due to: ", err)
					}
					return
				case kclActionTypeProcessRecords:
					r.LogDebug("Receieved ProcessRecords action from KCL")
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
		}()
	})
}

func (r *KclReader) shutdown() {
	defer func(){
		// Reset startupOnce to allow the start up sequence to happen again
		r.startupOnce = sync.Once{}
	}()

	if r.messages != nil {
		close(r.messages)
	}
}

// getAction reads messages from STDIN based on the Multilang Daemon protocol from KCL
func (r *KclReader) getAction() (*actionMessage, error) {
	buffer := &bytes.Buffer{}
	for {
		line, isPrefix, err := r.reader.ReadLine()
		if err != nil {
			r.LogError("Unable to read line from stdin ", err)
			return nil, err
		}
		buffer.Write(line)
		if !isPrefix {
			break
		}
	}

	actionMsg := &actionMessage{}
	err := json.Unmarshal(buffer.Bytes(), actionMsg)
	if err != nil {
		r.LogError("Unable to unmarshal line read from input: ", buffer.String())
		return nil, err
	}
	return actionMsg, nil
}

// sendMessage writes messages to STDOUT based on the Multilang Daemon protocol from KCL
func (r *KclReader) sendMessage(msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		r.LogError(err)
		return err
	}

	fmt.Fprintln(os.Stdout, string(b))

	return nil
}

// processRecords is a helper method which pulls from the reader's message channel and calls the callback function
func (r *KclReader) processRecords(ctx context.Context, batchSize int, fn messageHandler) (count int, size int, err error) {
	r.process(ctx)
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

// GetRecord calls processRecords to attempt to put one message from message buffer to the consumer's message
// channel
func (r *KclReader) GetRecord(ctx context.Context, fn messageHandler) (count int, size int, err error) {
	count, size, err = r.processRecords(ctx, 1, fn)
	return count, size, err
}

// GetRecords calls processRecords to attempt to put all messages on the message buffer on the consumer's
// message channel
func (r *KclReader) GetRecords(ctx context.Context, fn messageHandler) (count int, size int, err error) {
	count, size, err = r.processRecords(ctx, r.batchSize, fn)
	return count, size, err
}
