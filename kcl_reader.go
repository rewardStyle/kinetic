package kinetic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
)

type kclReaderOptions struct {
	onInitCallbackFn       func() error
	onCheckpointCallbackFn func() error
	onShutdownCallbackFn   func() error
	logLevel               aws.LogLevelType       // log level for configuring the LogHelper's log level
	Stats                  ConsumerStatsCollector // stats collection mechanism
}

func defaultKlcReaderOptions() *kclReaderOptions {
	return &kclReaderOptions{
		onInitCallbackFn:       func() error { return nil },
		onCheckpointCallbackFn: func() error { return nil },
		onShutdownCallbackFn:   func() error { return nil },
		logLevel:               aws.LogOff,
		Stats:                  &NilConsumerStatsCollector{},
	}
}

type KlcReaderOptionsFn func(*kclReaderOptions) error

func KlcReaderOnInitCallbackFn(fn func() error) KlcReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.onInitCallbackFn = fn
		return nil
	}
}

func KlcReaderOnCheckpointCallbackFn(fn func() error) KlcReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.onCheckpointCallbackFn = fn
		return nil
	}
}

func KlcReaderOnShutdownCallbackFn(fn func() error) KlcReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.onShutdownCallbackFn = fn
		return nil
	}
}

func KlcReaderLogLevel(ll aws.LogLevelType) KlcReaderOptionsFn {
	return func(o *kclReaderOptions) error {
		o.logLevel = ll
		return nil
	}
}

func KlcReaderStats(sc ConsumerStatsCollector) KlcReaderOptionsFn {
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
	scanner     *bufio.Scanner
	reader      *bufio.Reader
	msgBuffer   []Message
}

// NewKclReader creates a new stream reader to read records from KCL
func NewKclReader(c *aws.Config, optionFns ...KlcReaderOptionsFn) (*KclReader, error) {
	kclReaderOptions := defaultKlcReaderOptions()
	for _, optionFn := range optionFns {
		optionFn(kclReaderOptions)
	}
	return &KclReader{
		msgBuffer:        []Message{},
		kclReaderOptions: kclReaderOptions,
		LogHelper: &LogHelper{
			LogLevel: kclReaderOptions.logLevel,
			Logger:   c.Logger,
		},
	}, nil
}

// processRecords is a helper method which loops through the message buffer and puts messages on the listener's
// message channel.  After all the messages on the message buffer have been moved to the listener's message
// channel, a message is sent (following the Multilang protocol) to acknowledge that the processRecords message
// has been received / processed
func (r *KclReader) processRecords(fn MessageHandler, numRecords int) (int, int, error) {
	// Define the batchSize
	batchSize := 0
	if len(r.msgBuffer) > 0 {
		if numRecords < 0 {
			batchSize = len(r.msgBuffer)
		} else {
			batchSize = int(math.Min(float64(len(r.msgBuffer)), float64(numRecords)))
		}
	}
	r.Stats.AddBatchSize(batchSize)

	// TODO: Define the payloadSize
	var payloadSize int

	// Loop through the message buffer and call the message handler function on each message
	var wg sync.WaitGroup
	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go fn(&r.msgBuffer[0], &wg)
		r.msgBuffer = r.msgBuffer[1:]
		r.Stats.AddConsumed(1)
	}
	wg.Wait()

	// Send an acknowledgement that the 'ProcessRecords' message was received/processed
	if len(r.msgBuffer) == 0 {
		err := r.sendMessage(NewStatusMessage(PROCESSRECORDS))
		if err != nil {
			r.LogError(err)
			return batchSize, payloadSize, err
		}
	}

	return batchSize, payloadSize, nil
}

func (r *KclReader) getAction() (*ActionMessage, error) {
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

	actionMsg := &ActionMessage{}
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
		case INITIALIZE:
			r.onInit()
			r.sendMessage(NewStatusMessage(INITIALIZE))
		case CHECKPOINT:
			r.onCheckpoint()
			r.sendMessage(NewStatusMessage(CHECKPOINT))
		case SHUTDOWN:
			r.onShutdown()
			r.sendMessage(NewStatusMessage(SHUTDOWN))
		case PROCESSRECORDS:
			go func() error {
				for _, msg := range actionMessage.Records {
					r.msgBuffer = append(r.msgBuffer, *msg.ToMessage())
				}

				return nil
			}()
		default:
		}
	}

	return nil
}

func (r *KclReader) sendMessage(msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		r.LogError(err)
		return err
	}

	fmt.Fprintln(os.Stdout, string(b))

	return nil
}

func (r *KclReader) onInit() error {
	if r.onInitCallbackFn != nil {
		err := r.onInitCallbackFn()
		if err != nil {
			r.LogError(err)
			return err
		}
	}
	return nil
}

func (r *KclReader) onCheckpoint() error {
	if r.onCheckpointCallbackFn != nil {
		err := r.onCheckpointCallbackFn()
		if err != nil {
			r.LogError(err)
			return err
		}
	}
	return nil
}

func (r *KclReader) onShutdown() error {
	if r.onShutdownCallbackFn != nil {
		err := r.onShutdownCallbackFn()
		if err != nil {
			r.LogError(err)
			return err
		}
	}
	return nil
}

// GetRecord calls processRecords to attempt to put one message from message buffer to the listener's message
// channel
func (r *KclReader) GetRecord(ctx context.Context, fn MessageHandler) (int, int, error) {
	return r.processRecords(fn, 1)
}

// GetRecords calls processRecords to attempt to put all messages on the message buffer on the listener's
// message channel
func (r *KclReader) GetRecords(ctx context.Context, fn MessageHandler) (int, int, error) {
	return r.processRecords(fn, -1)
}
