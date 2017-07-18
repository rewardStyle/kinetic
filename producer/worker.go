package producer

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/rewardStyle/kinetic/message"
)

const (
	workerStateActive 	   = "active"
	workerStateIdle 	   = "idle"
	workerStateIdleWithRetries = "idleWithErrors"
	workerStateBusy 	   = "busy"
	workerStateDecommissioned  = "decommissioned"
	workerStateInactive 	   = "inactive"
)

// statusReport is used to communicate a worker's status to the producer.
type statusReport struct {
	workerID string		// uuid of the worker making the work request
	capacity int		// maximum message capacity the worker can handle
	timeout  time.Duration  // maximum time duration to wait for incoming messages
	failed   int            // number of previous messages that failed to send
}

// workerCommand is used by the producer to send commands to the workers.
type workerCommand struct {
	batchMsgs      []*message.Message // batch of messages for the worker to send
	decommissioned bool               // whether or not to decommission the worker
}

// worker is an object that, when started, runs in a separate go routine handling commands from the producer through
// the worker's command channel.  The worker communicates its status to the producer and the producer responds with
// a batch of messages for the worker to send.  The worker calls a closure (defined by the producer) to send a batch
// of messages containing a combination of new messages and previously failed messages.  The worker may also receive
// a command to decommission itself if the worker has no previously failed messages.
type worker struct {
	workerID       string			// unique ID to identify itself from the other workers
	state          string			// current state of the worker
	stateMu        sync.Mutex               // mutex for protecting against concurrent changes of state
	batchSize      int			// maximum number of message to be sent per batch
	batchTimeout   time.Duration            // maximum duration with which to wait for a batch of new messages
	sendBatchFn    sendBatchFn		// a closure to call when a batch is ready to be sent
	reportStatusFn reportStatusFn		// a clousre to call when the worker has a status update
	retries        []*message.Message	// a slice of messages that previously failed
	commands       chan *workerCommand	// a channel to which the producer sends commands
}

// newWorker creates a new worker object that automatically starts working.
func newWorker(cfg *producerOptions, sendBatchFn sendBatchFn, reportStatusFn reportStatusFn) *worker {
	var w *worker
	defer func() {
		go w.start()
	}()

	w = &worker{
		workerID: generateWorkerID(),
		state: workerStateActive,
		batchSize: cfg.batchSize,
		batchTimeout: cfg.batchTimeout,
		sendBatchFn: sendBatchFn,
		reportStatusFn: reportStatusFn,
		commands: make(chan *workerCommand),
	}

	return w
}

// start is called when a new worker is instantiated via newWorker.  start initiates an infinite loop that communicates
// the worker's status to the producer and listens to its command channel for commands sent by the producer
func (w *worker) start() {
	defer w.shutdown()

	var failed int
	for {
		// Update worker state
		if failed > 0 {
			w.setWorkerState(workerStateIdleWithRetries)
		} else {
			w.setWorkerState(workerStateIdle)
		}

		// Send the dispatcher a status report with the worker's capacity
		go func() {
			req := &statusReport{
				workerID: w.workerID,
				capacity: w.batchSize - failed,
				timeout: w.batchTimeout,
				failed: failed,
			}
			w.reportStatusFn(req)
		}()

		// Listen to incoming commands
		cmd := <-w.commands
		if cmd.decommissioned {
			w.setWorkerState(workerStateDecommissioned)
			return
		}

		// Only send if there are any messages to send
		if len(cmd.batchMsgs) + len(w.retries) > 0 {
			w.setWorkerState(workerStateBusy)

			// Combine new batch messages with previously failed messages
			var batch []*message.Message
			batch = append(batch, w.retries...)
			batch = append(batch, cmd.batchMsgs...)

			// Send batch and reset retries
			w.retries = w.sendBatchFn(batch)
		}
	}
}

// shutdown initiates the shutdown procedure for a worker
func (w *worker) shutdown() {
	// Close the command channel to prevent any more incoming commands
	if w.commands != nil {
		close(w.commands)
	}

	// Update the worker state to inactive
	w.setWorkerState(workerStateInactive)
}

// getWorkerState returns the worker's current state
func (w *worker) getWorkerState() string {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	return w.state
}

// setWorkerStatus updates the worker's current state
func (w *worker) setWorkerState(state string) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	w.state = state
}

// generateWorkerID is a helper function that generates UUIDs that are used as unique worker IDs
func generateWorkerID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("Error: ", err)
		return ""
	}
	return fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
