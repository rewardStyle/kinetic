package kinetic

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	NullStreamError = errors.New("A stream must be specified!")
	NotActiveError  = errors.New("The Stream is not yet active!")
)

type Listener struct {
	*Kinesis

	wg          sync.WaitGroup
	msgCount    int
	errCount    int
	listeningMu sync.Mutex

	listening bool

	errors     chan error
	messages   chan *Message
	interrupts chan os.Signal
}

func (l *Listener) Init(stream, shard string) (*Listener, error) {
	var err error

	if stream == "" {
		return nil, NullStreamError
	}

	l.Kinesis, err = new(Kinesis).Init(stream, shard, KShardIteratorTypes[conf.Kinesis.ShardIteratorType])
	if err != nil {
		return nil, err
	}

	// Is the stream ready?
	active, err := l.checkActive()
	if err != nil || active != true {
		if err != nil {
			return nil, err
		} else {
			return nil, NotActiveError
		}
	}

	l.messages = make(chan *Message)
	l.errors = make(chan error)
	l.interrupts = make(chan os.Signal, 1)

	// Relay incoming interrupt signals to this channel
	signal.Notify(l.interrupts, os.Interrupt)

	// Start feeder consumer
	go l.consume()

	return l, err
}

func (l *Listener) setListening(listening bool) {
	l.listeningMu.Lock()
	l.listening = listening
	l.listeningMu.Unlock()
}

func (l *Listener) IsListening() bool {
	l.listeningMu.Lock()
	defer l.listeningMu.Unlock()
	return l.listening
}

func (l *Listener) shouldConsume() bool {
	select {
	case <-l.interrupts:
		return false
	default:
		return true
	}

}

// Call GetRecords continuously
func (l *Listener) Listen(fn msgFn) {
	l.setListening(true)
stop:
	for {
		select {
		case err := <-l.Errors():
			l.errCount++
			l.wg.Add(1)
			go l.handleError(err)
		case msg := <-l.Messages():
			l.msgCount++
			l.wg.Add(1)
			go l.handleMsg(msg, fn)
		case sig := <-l.interrupts:
			l.handleInterrupt(sig)
			break stop
		}
	}
	l.setListening(false)
}

func (l *Listener) addMessage(msg *Message) {
	l.messages <- msg
}

func (l *Listener) consume() {
	counter := 0
	timer := time.Now()

	for {
		l.throttle(&counter, &timer)

		// args() will give us the shard iterator and type as well as the shard id
		response, err := l.client.GetRecords(l.args())
		if err != nil {
			l.errors <- err
		}

		if response != nil {
			for _, record := range response.Records {
				l.addMessage(&Message{&record})
			}

			l.setShardIterator(response.NextShardIterator)
		}

		if !l.shouldConsume() {
			break
		}
	}
}

func (l *Listener) Retrieve() (*Message, error) {
	select {
	case msg := <-l.Messages():
		return msg, nil
	case err := <-l.Errors():
		return nil, err
	case sig := <-l.interrupts:
		l.handleInterrupt(sig)
		return nil, nil
	}
}

func (l *Listener) RetrieveFn(fn msgFn) {
	select {
	case err := <-l.Errors():
		l.wg.Add(1)
		go l.handleError(err)
	case msg := <-l.Messages():
		l.wg.Add(1)
		go fn(msg.Value(), &l.wg)
	case sig := <-l.interrupts:
		l.handleInterrupt(sig)
	}
}

func (l *Listener) Close() error {
	if conf.Debug.Verbose {
		log.Println("Waiting for all tasks to finish...")
	}

	// Stop consuming
	go func() {
		l.interrupts <- syscall.SIGINT
	}()

	l.wg.Wait()

	if conf.Debug.Verbose {
		log.Println("Done.")
	}

	return nil
}

func (l *Listener) Errors() <-chan error {
	return l.errors
}

func (l *Listener) Messages() <-chan *Message {
	return l.messages
}

func (l *Listener) handleMsg(msg *Message, fn msgFn) {
	if conf.Debug.Verbose {
		log.Printf("Received message: %s with key: %s", msg.Value(), msg.Key())
		log.Printf("Messages received: %d", l.msgCount)
	}

	fn(msg.Value(), &l.wg)
}

func (l *Listener) handleInterrupt(signal os.Signal) {
	if conf.Debug.Verbose {
		log.Println("Received interrupt signal")
	}

	l.Close()
}

func (l *Listener) handleError(err error) {
	if err != nil && conf.Debug.Verbose {
		log.Println("Received error: ", err.Error())
	}

	l.wg.Done()
}

// Kinesis allows five read ops per second per shard
// http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html
func (l *Listener) throttle(counter *int, timer *time.Time) {
	// If a second has passed since the last timer start, reset the timer
	if time.Now().After(timer.Add(1 * time.Second)) {
		*timer = time.Now()
		*counter = 0
	}

	*counter++

	// If we have attempted five times and it has been less than one second
	// since we started reading then we need to wait for the second to finish
	if *counter >= 5 && !(time.Now().After(timer.Add(1 * time.Second))) {
		// Wait for the remainder of the second - timer and counter
		// will be reset on next pass
		<-time.After(1 * time.Second)
	}

}
