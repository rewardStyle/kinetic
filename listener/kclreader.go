package listener

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
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
		err := r.sendMessage(multilang.NewStatusMessage(multilang.ProcessRecords))
		if err != nil {
			r.listener.LogError(err)
			return batchSize, err
		}
	}

	return batchSize, nil
}

// processAction listens to STDIN and processes action messages based on the Multilanguage protocol from KCL
func (r *KclReader) processAction() error {
	for r.scanner.Scan() {
		actionMessage := &multilang.ActionMessage{}
		err := json.Unmarshal(r.scanner.Bytes(), actionMessage)
		if err != nil {
			return err
		}

		switch actionMessage.Action {
		case multilang.Initialize:
			r.onInit()
			r.sendMessage(multilang.NewStatusMessage(multilang.Initialize))
		case multilang.Checkpoint:
			r.onCheckpoint()
			r.sendMessage(multilang.NewStatusMessage(multilang.Checkpoint))
		case multilang.Shutdown:
			r.onShutdown()
			r.sendMessage(multilang.NewStatusMessage(multilang.Shutdown))
		case multilang.ProcessRecords:
			go func() error {
				r.mutex.Lock()
				defer r.mutex.Unlock()

				if r.ackPending {
					return errors.New("Receieved a processRecords action message from KCL " +
						"unexpectedly")
				}

				// Decode the base64 encoded message
				decodedMsg, err := base64.StdEncoding.DecodeString(actionMessage.Records)
				if err != nil {
					r.listener.LogError(err)
					return err
				}

				// Unmarshall the decoded message
				msgs := []message.Message{}
				err = json.Unmarshal(decodedMsg, msgs)
				if err != nil {
					r.listener.LogError(err)
					return err
				}

				r.msgBuffer = append(r.msgBuffer, msgs...)
				r.ackPending = true;

				return nil
			}()
		default:
		}
	}

	return nil
}

func (r *KclReader) sendMessage(msg *multilang.ActionMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		r.listener.LogError(err)
		return err
	}
	_, err = os.Stdout.Write(b)
	if err != nil {
		r.listener.LogError(err)
		return err
	}

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
