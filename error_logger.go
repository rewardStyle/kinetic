package kinetic

import (
	"log"
	"sync"
	"time"
)

type ErrorRecord struct {
	err error
	ts  time.Time
}

type ErrorLogger struct {
	errors      []ErrorRecord
	channel     chan ErrorRecord
	maxEntries  int
	maxDuration time.Duration
	timer       time.Time
	pod         chan struct{}
	wg          sync.WaitGroup
	isRunningMu sync.Mutex
	isRunning   bool
	runRequests int
}

func (e *ErrorLogger) init(maxEntries int, maxDuration time.Duration) (*ErrorLogger, error) {
	var err error
	e.maxEntries = maxEntries
	e.maxDuration = maxDuration
	e.channel = make(chan ErrorRecord, maxEntries)
	e.pod = make(chan struct{})
	return e, err
}

func (e *ErrorLogger) Init(maxEntries int, maxDuration time.Duration) (*ErrorLogger, error) {
	return e.init(maxEntries, maxDuration)
}

func (e *ErrorLogger) StartLogging() {
	e.isRunningMu.Lock()
	defer e.isRunningMu.Unlock()
	e.runRequests++
	if e.isRunning {
		return
	}
	e.isRunning = true

	e.wg.Add(1)
	e.timer = time.Now()
	go func() {
	stop:
		for {
			select {
			case <-e.pod:
				break stop
			case rec := <-e.channel:
				e.errors = append(e.errors, rec)
			}
			if len(e.errors) >= e.maxEntries || time.Now().After(e.timer.Add(e.maxDuration*time.Second)) {
				for _, rec := range e.errors {
					log.Println(rec.ts.String(), " - Received error: ", rec.err.Error())
				}
				e.errors = nil
				e.timer = time.Now()
			}
		}
		e.wg.Done()
	}()
}

func (e *ErrorLogger) StopLogging() {
	e.isRunningMu.Lock()
	defer e.isRunningMu.Unlock()
	if !e.isRunning {
		return
	}
	e.runRequests--
	if e.runRequests == 0 {
		e.pod <- struct{}{}
		e.wg.Wait()
		e.isRunning = false
	}
}

func (e *ErrorLogger) AddError(err error) {
	go func() {
		e.channel <- ErrorRecord{err, time.Now()}
	}()
}

var globalErrorLogger *ErrorLogger

func init() {
	globalErrorLogger, _ = new(ErrorLogger).Init(1000, 30*time.Second)
}
