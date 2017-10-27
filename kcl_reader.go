package kinetic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	batchSize                int                        // maximum records per GetRecords call
	reader                   io.Reader                  // reader for reading from KCL
	writer                   io.Writer                  // writer for writing to KCL
	autoCheckpointCount      int                        // maximum number of messages pulled off the message queue before triggering an auto checkpoint
	autoCheckpointFreq       time.Duration              // frequency with which to auto checkpoint
	updateCheckpointSizeFreq time.Duration              // frequency with which to update the CheckpointSize stats
	onInitCallbackFn         func() error               // callback function that gets called after an initialize action message is sent from KCL
	onCheckpointCallbackFn   func(string, string) error // callback function that gets called after a checkpoint action message is sent from KCL
	onShutdownCallbackFn     func() error               // callback function that gets called after shutdown action message is sent from KCL
	logLevel                 aws.LogLevelType           // log level for configuring the LogHelper's log level
	Stats                    ConsumerStatsCollector     // stats collection mechanism
}

// defaultKclReaderOptions instantiates a kclReaderOptions with default values.
func defaultKclReaderOptions() *kclReaderOptions {
	return &kclReaderOptions{
		batchSize:                kclReaderMaxBatchSize,
		reader:                   os.Stdin,
		writer:                   os.Stdout,
		autoCheckpointCount:      10000,
		autoCheckpointFreq:       time.Minute,
		updateCheckpointSizeFreq: time.Minute,
		onInitCallbackFn:         func() error { return nil },
		onCheckpointCallbackFn:   func(a, b string) error { return nil },
		onShutdownCallbackFn:     func() error { return nil },
		logLevel:                 aws.LogOff,
		Stats:                    &NilConsumerStatsCollector{},
	}
}

// KclReaderOptionsFn is a method signature for defining functional option methods for configuring the KclReader.
type KclReaderOptionsFn func(*KclReader) error

// KclReaderBatchSize is a functional option method for configuring the KclReader's batch size
func KclReaderBatchSize(size int) KclReaderOptionsFn {
	return func(o *KclReader) error {
		if size > 0 && size <= kclReaderMaxBatchSize {
			o.batchSize = size
			return nil
		}
		return ErrInvalidBatchSize
	}
}

// KclReaderReader is a functional option method for configuring the KclReader's reader (defaults to os.Stdin).
func KclReaderReader(reader io.Reader) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.reader = reader
		return nil
	}
}

// KclReaderWriter is a functional option method for configuring the KclReader's writer (defaults to os.Stdout).
func KclReaderWriter(writer io.Writer) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.writer = writer
		return nil
	}
}

// KclReaderAutoCheckpointCount is a functional option method for configuring the KclReader's checkpoint count
func KclReaderAutoCheckpointCount(count int) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.autoCheckpointCount = count
		return nil
	}
}

// KclReaderAutoCheckpointFreq is a functional option method for configuring the KclReader's checkpoint frequency
func KclReaderAutoCheckpointFreq(freq time.Duration) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.autoCheckpointFreq = freq
		return nil
	}
}

// KclReaderUpdateCheckpointSizeFreq is a functional option method for configuring the KclReader's
// update checkpoint size stats frequency
func KclReaderUpdateCheckpointSizeFreq(freq time.Duration) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.updateCheckpointSizeFreq = freq
		return nil
	}
}

// KclReaderOnInitCallbackFn is a functional option method for configuring the KclReader's
// onInitCallbackFn.
func KclReaderOnInitCallbackFn(fn func() error) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.onInitCallbackFn = fn
		return nil
	}
}

// KclReaderOnCheckpointCallbackFn is a functional option method for configuring the KclReader's
// onCheckpointCallbackFn.
func KclReaderOnCheckpointCallbackFn(fn func(seqNum string, err string) error) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.onCheckpointCallbackFn = fn
		return nil
	}
}

// KclReaderOnShutdownCallbackFn is a functional option method for configuring the KclReader's
// onShutdownCallbackFn.
func KclReaderOnShutdownCallbackFn(fn func() error) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.onShutdownCallbackFn = fn
		return nil
	}
}

// KclReaderLogLevel is a functional option method for configuring the KclReader's log level.
func KclReaderLogLevel(ll aws.LogLevelType) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.logLevel = ll
		return nil
	}
}

// KclReaderStats is a functional option method for configuring the KclReader's stats collector.
func KclReaderStats(sc ConsumerStatsCollector) KclReaderOptionsFn {
	return func(o *KclReader) error {
		o.Stats = sc
		return nil
	}
}

// KclReader handles the KCL Multilang Protocol to read records from KCL
type KclReader struct {
	*kclReaderOptions                // contains all of the configuration settings for the KclReader
	*LogHelper                       // object for help with logging
	bufReader    *bufio.Reader       // buffered reader to read messages from KCL
	burWriter    *bufio.Writer       // buffered writer to write messages to KCL
	checkpointer *checkpointer       // data structure used to manage checkpointing
	ticker       *time.Ticker        // a ticker with which to update the CheckpointSize stats
	tickerDone   chan empty          // a channel used to communicate when to stop updating the CheckpointSize stats
	messages     chan *Message       // unbuffered message channel used to throttle the record processing from KCL
	actions      chan *actionMessage // unbuffered action message channel used internally to coordinate sending action messages to KCL
	startupOnce  sync.Once           // used to ensure that the startup function is called once
	shutdownOnce sync.Once           // used to ensure that the shutdown function is called once
}

// NewKclReader creates a new stream reader to read records from KCL
func NewKclReader(c *aws.Config, optionFns ...KclReaderOptionsFn) (*KclReader, error) {
	kclReader := &KclReader{kclReaderOptions: defaultKclReaderOptions()}
	for _, optionFn := range optionFns {
		optionFn(kclReader)
	}

	// Setup a buffered reader/writer from the io reader/writer for communicating via the Multilang Daemon Protocol
	kclReader.bufReader = bufio.NewReader(kclReader.reader)
	kclReader.burWriter = bufio.NewWriter(kclReader.writer)

	kclReader.LogHelper = &LogHelper{
		LogLevel: kclReader.logLevel,
		Logger:   c.Logger,
	}

	return kclReader, nil
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
		r.tickerDone = make(chan empty)

		// instantiate and start the checkpointer
		r.checkpointer = newCheckpointer(
			checkpointAutoCheckpointCount(r.autoCheckpointCount),
			checkpointAutoCheckpointFreq(r.autoCheckpointFreq),
			checkpointCheckpointFn(func(checkpoint string) error {
				r.actions <- newCheckpointMessage(checkpoint)
				r.Stats.AddCheckpointSent(1)
				return nil
			}),
		)
		r.checkpointer.startup(ctx)

		// send messages to KCL
		go func() {
			for actionMessage := range r.actions {
				r.LogInfo(fmt.Sprintf("Sending a %s action message to KCL", actionMessage.Action))
				err := r.sendToStdOut(actionMessage)
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
				actionMessage, err := r.receiveFromStdIn()
				if err != nil || actionMessage == nil {
					panic(err)
				}

				switch actionMessage.Action {
				case kclActionTypeInitialize:
					r.LogDebug("Receieved Initialize action from KCL")
					r.onInitCallbackFn()
					r.actions <- newStatusMessage(actionMessage.Action)
				case kclActionTypeCheckpoint:
					r.LogDebug("Receieved Checkpoint action from KCL")
					if actionMessage.Error == "" {
						r.LogInfo("Checkpoint acknowledgement from KCL was successful "+
							"for sequence number: ", actionMessage.SequenceNumber)
						r.Stats.AddCheckpointSuccess(1)
					} else {
						r.LogError("Checkpoint acknowledgement from KCL had an error: ",
							actionMessage.Error)
						r.Stats.AddCheckpointError(1)
					}
					r.onCheckpointCallbackFn(actionMessage.SequenceNumber, actionMessage.Error)
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

		// Periodically update the CheckpointSize stats
		go func() {
			r.ticker = time.NewTicker(r.updateCheckpointSizeFreq)
			defer r.ticker.Stop()

			select {
			case <-r.ticker.C:
				r.Stats.UpdateCheckpointSize(r.checkpointer.size())
			case <-r.tickerDone:
				return
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

	if r.actions != nil {
		close(r.actions)
	}

	if r.tickerDone != nil {
		close(r.tickerDone)
	}
}

// receiveFromStdIn reads messages from STDIN based on the Multilang Daemon protocol from KCL
func (r *KclReader) receiveFromStdIn() (*actionMessage, error) {
	buffer := &bytes.Buffer{}
	for {
		line, isPrefix, err := r.bufReader.ReadLine()
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

// sendToStdOut writes messages to STDOUT based on the Multilang Daemon protocol from KCL
func (r *KclReader) sendToStdOut(msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		r.LogError(err)
		return err
	}

	r.burWriter.Write(b)
	fmt.Println(r.burWriter, string(b))
	r.burWriter.Flush()

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
func (r *KclReader) GetRecord(ctx context.Context, fn messageHandler) error {
	_, _, err := r.processRecords(ctx, 1, fn)
	return err
}

// GetRecords calls processRecords to attempt to put all messages on the message buffer on the consumer's
// message channel
func (r *KclReader) GetRecords(ctx context.Context, fn messageHandler) error {
	_, _, err := r.processRecords(ctx, r.batchSize, fn)
	return err
}

// Checkpoint sends a message to KCL if there is sequence number that can be checkpointed
func (r *KclReader) Checkpoint() error {
	return r.checkpointer.checkpoint()
}

// CheckpointInsert registers a sequence number with the checkpointer
func (r *KclReader) CheckpointInsert(seqNum string) error {
	err := r.checkpointer.insert(seqNum)
	if err != nil {
		return err
	}
	r.Stats.AddCheckpointInsert(1)
	return nil
}

// CheckpointDone marks the given sequence number as done in the checkpointer
func (r *KclReader) CheckpointDone(seqNum string) error {
	err := r.checkpointer.markDone(seqNum)
	if err != nil {
		return err
	}
	r.Stats.AddCheckpointDone(1)
	return nil
}
