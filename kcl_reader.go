package kinetic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

const (
	kclReaderMaxBatchSize = 10000
)

// kclReaderOptions is a struct that holds all of the KclReader's configurable parameters.
type kclReaderOptions struct {
	batchSize              int
	autoCheckpointCount    int                    // maximum number of messages pulled off the message queue before triggering an auto checkpoint
	autoCheckpointFreq     time.Duration          // frequency with which to auto checkpoint
	checkpointMaxAge       time.Duration          // maximum duration for which a sequence number lives in the checkpoint system
	checkpointMaxSize      int                    // maximum records per GetRecordsRequest call
	onInitCallbackFn       func() error           // callback function that gets called after initialization
	onCheckpointCallbackFn func() error           // callback function that gets called after checkpointing
	onShutdownCallbackFn   func() error           // callback function that gets called after shutdown
	logLevel               aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats                  ConsumerStatsCollector // stats collection mechanism
}

// defaultKclReaderOptions instantiates a kclReaderOptions with default values.
func defaultKclReaderOptions() *kclReaderOptions {
	return &kclReaderOptions{
		batchSize:           kclReaderMaxBatchSize,
		autoCheckpointCount: 10000,
		autoCheckpointFreq:  time.Minute,
		checkpointMaxSize:   1000000,
		checkpointMaxAge:    time.Hour,
		logLevel:            aws.LogOff,
		Stats:               &NilConsumerStatsCollector{},
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

// KclReaderAutoCheckpointFreq is a functional option method for configuring the KclReader's checkpoint frequency
func KclReaderAutoCheckpointFreq(freq time.Duration) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.autoCheckpointFreq = freq
		return nil
	}
}

// KclReaderAutoCheckpointCount is a functional option method for configuring the KclReader's checkpoint count
func KclReaderAutoCheckpointCount(count int) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.autoCheckpointCount = count
		return nil
	}
}

// KclReaderCheckpointMaxAge is a functional option method for configuring the KclReader's checkpoint max age
func KclReaderCheckpointMaxAge(age time.Duration) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.checkpointMaxAge = age
		return nil
	}
}

// KclReaderCheckpointMaxSize is a functional option method for configuring the KclReader's checkpoint max age
func KclReaderCheckpointMaxSize(size int) KclReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.checkpointMaxSize = size
		return nil
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
	*kclReaderOptions                     // contains all of the configuration settings for the KclReader
	*LogHelper                            // object for help with logging
	reader            *bufio.Reader       // io reader to read from STDIN
	checkpoint        *checkpoint         // data structure used to manage checkpointing
	messages          chan *Message       // unbuffered message channel used to throttle the record processing from KCL
	actions           chan *actionMessage // unbuffered action message channel used internally to coordinate sending action messages to KCL
	startupOnce       sync.Once           // used to ensure that the startup function is called once
	shutdownOnce      sync.Once           // used to ensure that the shutdown function is called once
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
	r.startupOnce.Do(func() {
		defer func() {
			// Reset shutdownOnce to allow the shut down sequence to happen again
			r.shutdownOnce = sync.Once{}
		}()

		// create communication channels
		r.messages = make(chan *Message)
		r.actions = make(chan *actionMessage)

		// instantiate and start the checkpointing system
		r.checkpoint = newCheckpoint(
			checkpointAutoCheckpointCount(r.autoCheckpointCount),
			checkpointAutoCheckpointFreq(r.autoCheckpointFreq),
			checkpointMaxAge(r.checkpointMaxAge),
			checkpointMaxSize(r.checkpointMaxSize),
			checkpointCheckpointFn(func(checkpoint string) error {
				r.actions <- newCheckpointMessage(checkpoint)
				return nil
			}),
			checkpointExpireFn(func(checkpoint string) error {
				r.LogError(fmt.Sprintf("Checkpoint: Sequence number [%s] exceeded max age", checkpoint))
				return nil
			}),
			checkpointCapacityFn(func(checkpoint string) error {
				r.LogError(fmt.Sprintf("Checkpoint: Sequence number [%s] exceeded max size", checkpoint))
				return nil
			}),
		)
		r.checkpoint.startup(ctx)

		// send messages to KCL
		go func() {
			for {
				actionMessage := <-r.actions
				r.LogInfo(fmt.Sprintf("Sending a %s action message to KCL", actionMessage.Action))
				err := r.sendMessage(actionMessage)
				if err != nil {
					r.LogError(fmt.Sprintf("Unable to send %s action message due to: ",
						actionMessage.Action), err)
				}
			}
		}()

		// listen to STDIN and processes action messages based on the Multilang protocol from KCL
		go func() {
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
					r.actions <- newStatusMessage(actionMessage.Action)
				case kclActionTypeCheckpoint:
					r.LogDebug("Receieved Checkpoint action from KCL")
					r.onCheckpointCallbackFn()
					r.actions <- newStatusMessage(actionMessage.Action)
				case kcActionTypeShutdown:
					r.LogDebug("Receieved Shutdown action from KCL")
					r.onShutdownCallbackFn()
					r.actions <- newStatusMessage(actionMessage.Action)
				case kclActionTypeProcessRecords:
					r.LogDebug("Receieved ProcessRecords action from KCL")
					// Put the messages on the reader's message channel
					// (one by one as they are pulled off and processed by the reader)
					go func() {
						for _, msg := range actionMessage.Records {
							r.messages <- msg.ToMessage()
						}
						r.actions <- newStatusMessage(actionMessage.Action)
					}()
				default:
					r.LogError("processAction received an invalid action: ", actionMessage.Action)
				}
			}
		}()
	})
}

func (r *KclReader) shutdown() {
	defer func() {
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
		// pull a message off of the reader's message channel
		msg := <-r.messages

		// execute the caller's callback function for processing the message
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

// Checkpoint sends a message to KCL if there is sequence number that can be checkpointed
func (r *KclReader) Checkpoint() error {
	return r.checkpoint.checkpoint()
}

// CheckpointInsert registers a sequence number with the checkpointing system
func (r *KclReader) CheckpointInsert(seqNum string) error {
	return r.checkpoint.insert(seqNum)
}

// CheckpointDone marks the given sequence number as done in the checkpointing system
func (r *KclReader) CheckpointDone(seqNum string) error {
	return r.checkpoint.markDone(seqNum)
}
