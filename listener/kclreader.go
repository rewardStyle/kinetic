package listener

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/multilang"
)

type kclReaderOptions struct {
	onInitCallbackFn       func() error
	onCheckpointCallbackFn func() error
	onShutdownCallbackFn   func() error
}

// KclReader handles the KCL Multilang Protocol to read records from KCL
type KclReader struct {
	*kclReaderOptions
	throttleSem chan Empty
	listener    *Listener
	scanner     *bufio.Scanner
	reader      *bufio.Reader
	mutex       *sync.Mutex
	msgBuffer   []message.Message
	ackPending  bool
}

// NewKclReader creates a new stream reader to read records from KCL
func NewKclReader(fn ...func(*KclReaderConfig)) *KclReader {
	config := NewKclReaderConfig()
	for _, f := range fn {
		f(config)
	}
	return &KclReader{
		kclReaderOptions: config.kclReaderOptions,
		throttleSem: make(chan Empty, 5),
		msgBuffer: []message.Message{},
		mutex: &sync.Mutex{},
	}
}

// AssociateListener associates the KCL stream reader to a listener
func (r *KclReader) AssociateListener(l *Listener) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.listener != nil {
		return errs.ErrListenerAlreadyAssociated
	}
	r.listener = l
	return nil
}

// ensureClient will lazily ensure that we are reading from STDIN.
func (r *KclReader) ensureClient() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.scanner == nil {
		if r.listener == nil {
			return errs.ErrNilListener
		}
		r.scanner = bufio.NewScanner(os.Stdin)
		r.reader = bufio.NewReader(os.Stdin)
		go func() error {
			return r.processAction()
		}()
	}
	return nil
}

// GetRecord calls processRecords to attempt to put one message from message buffer to the listener's message
// channel
func (r *KclReader) GetRecord() (int, error) {
	return r.processRecords(1)
}

// GetRecords calls processRecords to attempt to put all messages on the message buffer on the listener's
// message channel
func (r *KclReader) GetRecords() (int, error) {
	return r.processRecords(-1)
}

// processRecords is a helper method which loops through the message buffer and puts messages on the listener's
// message channel.  After all the messages on the message buffer have been moved to the listener's message
// channel, a message is sent (following the Multilang protocol) to acknowledge that the processRecords message
// has been received / processed
func (r *KclReader) processRecords(numRecords int) (int, error) {
	if err := r.ensureClient(); err != nil {
		return 0, err
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

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
	for i := 0; i < batchSize; i++ {
		r.listener.messages <- &r.msgBuffer[0]
		r.msgBuffer = r.msgBuffer[1:]
	}

	// Send an acknowledgement that the 'ProcessRecords' message was received/processed
	if len(r.msgBuffer) == 0 && r.ackPending {
		err := r.sendMessage(multilang.NewStatusMessage(multilang.PROCESSRECORDS))
		if err != nil {
			r.listener.LogError(err)
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
				r.mutex.Lock()
				defer r.mutex.Unlock()

				if r.ackPending {
					return errors.New("Received a processRecords action message from KCL " +
						"unexpectedly")
				}

				for _, msg := range actionMessage.Records {
					r.msgBuffer = append(r.msgBuffer, *msg.ToMessage())
				}
				r.ackPending = true;

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
		r.listener.LogError(err)
		return err
	}

	fmt.Fprintln(os.Stdout, string(b))

	return nil
}

func (r *KclReader) onInit() error {
	if r.onInitCallbackFn != nil {
		err := r.onInitCallbackFn()
		if err != nil {
			r.listener.LogError(err)
			return err
		}
	}
	return nil
}

func (r *KclReader) onCheckpoint() error {
	if r.onCheckpointCallbackFn != nil {
		err := r.onCheckpointCallbackFn()
		if err != nil {
			r.listener.LogError(err)
			return err
		}
	}
	return nil
}

func (r *KclReader) onShutdown() error {
	if r.onShutdownCallbackFn != nil {
		err := r.onShutdownCallbackFn()
		if err != nil {
			r.listener.LogError(err)
			return err
		}
	}
	return nil
}
