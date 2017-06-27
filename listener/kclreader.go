package listener

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/multilang"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/aws/aws-sdk-go/aws"
)

type kclReaderOptions struct {
	onInitCallbackFn       func() error
	onCheckpointCallbackFn func() error
	onShutdownCallbackFn   func() error
	Stats                  StatsCollector

}

// KclReader handles the KCL Multilang Protocol to read records from KCL
type KclReader struct {
	*kclReaderOptions
	*logging.LogHelper

	throttleSem chan empty
	pipeOfDeath chan empty
	scanner     *bufio.Scanner
	reader      *bufio.Reader
	msgBuffer   []message.Message
}

// NewKclReader creates a new stream reader to read records from KCL
func NewKclReader(c *aws.Config, fn ...func(*KclReaderConfig)) (*KclReader, error) {
	cfg := NewKclReaderConfig(c)
	for _, f := range fn {
		f(cfg)
	}
	return &KclReader{
		kclReaderOptions: cfg.kclReaderOptions,
		LogHelper: &logging.LogHelper{
			LogLevel: cfg.LogLevel,
			Logger:  cfg.AwsConfig.Logger,
		},
		throttleSem: make(chan empty, 5),
		msgBuffer: []message.Message{},
	}, nil
}

// processRecords is a helper method which loops through the message buffer and puts messages on the listener's
// message channel.  After all the messages on the message buffer have been moved to the listener's message
// channel, a message is sent (following the Multilang protocol) to acknowledge that the processRecords message
// has been received / processed
func (r *KclReader) processRecords(fn MessageHandler, numRecords int) (int, error) {
	// Define the batchSize
	batchSize := 0;
	if len(r.msgBuffer) > 0 {
		if numRecords < 0 {
			batchSize = len(r.msgBuffer)
		} else {
			batchSize = int(math.Min(float64(len(r.msgBuffer)), float64(numRecords)))
		}
	}

	// Loop through the message buffer and put the correct number of messages on the listener's message channel
	var wg sync.WaitGroup
	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go fn(&r.msgBuffer[0], &wg)
		r.msgBuffer = r.msgBuffer[1:]
	}
	wg.Wait()

	// Send an acknowledgement that the 'ProcessRecords' message was received/processed
	if len(r.msgBuffer) == 0 {
		err := r.sendMessage(multilang.NewStatusMessage(multilang.PROCESSRECORDS))
		if err != nil {
			r.LogError(err)
			return batchSize, err
		}
	}

	return batchSize, nil
}

func (r *KclReader) getAction() (*multilang.ActionMessage, error) {
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

	actionMsg := &multilang.ActionMessage{}
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
		case multilang.INITIALIZE:
			r.onInit()
			r.sendMessage(multilang.NewStatusMessage(multilang.INITIALIZE))
		case multilang.CHECKPOINT:
			r.onCheckpoint()
			r.sendMessage(multilang.NewStatusMessage(multilang.CHECKPOINT))
		case multilang.SHUTDOWN:
			r.onShutdown()
			r.sendMessage(multilang.NewStatusMessage(multilang.SHUTDOWN))
		case multilang.PROCESSRECORDS:
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
func (r *KclReader) GetRecord(ctx context.Context,fn MessageHandler) (int, error) {
	return r.processRecords(fn, 1)
}

// GetRecords calls processRecords to attempt to put all messages on the message buffer on the listener's
// message channel
func (r *KclReader) GetRecords(ctx context.Context,fn MessageHandler) (int, error) {
	return r.processRecords(fn, -1)
}
