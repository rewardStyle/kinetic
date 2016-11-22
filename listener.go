package kinetic

import (
	"errors"
	"log"
	"net"
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
	*kinesis

	accessKey string
	secretKey string
	region    string

	concurrency   int
	concurrencyMu sync.Mutex
	sem           chan bool

	wg        sync.WaitGroup
	msgBuffer int

	listening   bool
	listeningMu sync.Mutex
	consuming   bool
	consumingMu sync.Mutex
	messageMu   sync.Mutex

	errors     chan error
	messages   chan *Message
	interrupts chan os.Signal

	errorLogger *ErrorLogger
}

func (l *Listener) init(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int, errorLogger *ErrorLogger) (*Listener, error) {
	var err error
	if concurrency < 1 {
		return nil, BadConcurrencyError
	}
	if stream == "" {
		return nil, NullStreamError
	}

	l.setConcurrency(concurrency)

	l.accessKey = accessKey
	l.secretKey = secretKey
	l.region = region
	l.errorLogger = errorLogger

	l.sem = make(chan bool, l.getConcurrency())
	l.errors = make(chan error, l.getConcurrency())
	l.messages = make(chan *Message, l.msgBufSize())

	l.interrupts = make(chan os.Signal, 1)
	signal.Notify(l.interrupts, os.Interrupt)

	l.kinesis, err = new(kinesis).init(stream, shard, shardIterType, accessKey, secretKey, region)
	if err != nil {
		return l, err
	}

	// Is the stream ready?
	active, err := l.checkActive()
	if err != nil || active != true {
		if err != nil {
			return l, err
		} else {
			return l, NotActiveError
		}
	}

	// Start feeder consumer
	go l.consume()

	return l, err
}

// Initialize a listener with the params specified in the configuration file
func (l *Listener) Init() (*Listener, error) {
	return l.init(conf.Kinesis.Stream, conf.Kinesis.Shard, ShardIterTypes[conf.Kinesis.ShardIteratorType], conf.AWS.AccessKey, conf.AWS.SecretKey, conf.AWS.Region, conf.Concurrency.Listener, globalErrorLogger)
}

// Initialize a listener with the supplied params
func (l *Listener) InitC(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int, errorLogger *ErrorLogger) (*Listener, error) {
	return l.init(stream, shard, shardIterType, accessKey, secretKey, region, concurrency, errorLogger)
}

// Re-initialize kinesis client with new endpoint. Used for testing with kinesalite
func (l *Listener) NewEndpoint(endpoint, stream string) {
	l.kinesis.client = l.kinesis.newClient(endpoint, stream)
	l.initShardIterator()

	if !l.IsConsuming() {
		go l.consume()
	}
}

func (l *Listener) setConcurrency(concurrency int) {
	l.concurrencyMu.Lock()
	l.concurrency = concurrency
	l.concurrencyMu.Unlock()
}

func (l *Listener) getConcurrency() int {
	l.concurrencyMu.Lock()
	defer l.concurrencyMu.Unlock()
	return l.concurrency
}

func (l *Listener) msgBufSize() int {
	l.concurrencyMu.Lock()
	defer l.concurrencyMu.Unlock()
	return l.concurrency * 1000
}

func (l *Listener) setListening(listening bool) {
	l.listeningMu.Lock()
	l.listening = listening
	l.listeningMu.Unlock()
}

// Identifies whether or not messages and errors are being handled after consumption
func (l *Listener) IsListening() bool {
	l.listeningMu.Lock()
	defer l.listeningMu.Unlock()
	return l.listening
}

func (l *Listener) setConsuming(consuming bool) {
	l.consumingMu.Lock()
	l.consuming = consuming
	l.consumingMu.Unlock()
}

// Identifies whether or not the kinesis stream is being polled
func (l *Listener) IsConsuming() bool {
	l.consumingMu.Lock()
	defer l.consumingMu.Unlock()
	return l.consuming
}

func (l *Listener) shouldConsume() bool {
	select {
	case <-l.interrupts:
		return false
	default:
		return true
	}

}

// Handle the consumed messages, errors and interrupts
func (l *Listener) Listen(fn msgFn) {
	l.setListening(true)
stop:
	for {
		getLock(l.sem)

		select {
		case err := <-l.Errors():
			l.incErrCount()
			l.wg.Add(1)
			go l.handleError(err)
		case msg := <-l.Messages():
			l.incMsgCount()
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
retry:
	l.messageMu.Lock()

	l.msgBuffer++

	// Allow reads to catch up to prevent deadlock
	if l.msgBuffer >= l.msgBufSize() {
		l.msgBuffer--
		l.messageMu.Unlock()

		<-time.After(1 * time.Millisecond)

		goto retry
	}

	l.messages <- msg
	l.messageMu.Unlock()
}

// Continually poll the Kinesis stream
func (l *Listener) consume() {
	l.setConsuming(true)

	readCounter := 0
	readTimer := time.Now()

	GsiCounter := 0
	GsiTimer := time.Now()

	for {
		if !l.shouldConsume() {
			l.setConsuming(false)
			break
		}

		l.throttle(&readCounter, &readTimer)

		// args() will give us the shard iterator and type as well as the shard id
		response, err := l.client.GetRecords(l.args())
		if err != nil {
			go func() {
				l.errors <- err
			}()

			// We receive net.OpError if kinesis terminates the socket.
			// It will contain a message resembling:
			//
			// Received error:  Post https://kinesis.us-east-1.amazonaws.com:
			// read tcp 172.16.0.38:37680->54.239.28.39:443: read: connection reset by peer
			//
			// If this happens we need to refresh the kinesis client to
			// reestablish the connection
			if _, ok := err.(*net.OpError); ok {
				log.Println("Received net.OpError. Recreating kinesis client and connection.")
				l.refreshClient(l.accessKey, l.secretKey, l.region)
			}

		refresh_iterator:

			// Refresh the shard iterator
			err := l.initShardIterator()
			if err != nil {
				log.Println("Failed to refresh iterator: " + err.Error())

				// If we received an error we should wait and attempt to
				// refresh the shard iterator again
				l.throttle(&GsiCounter, &GsiTimer)

				goto refresh_iterator
			}
		}

		if response != nil {
			l.setShardIterator(response.NextShardIterator)

			if len(response.Records) > 0 {
				for _, record := range response.Records {
					l.addMessage(&Message{record})
					l.setSequenceNumber(record.SequenceNumber)
				}
			}
		}
	}
}

// Retrive a message from the stream and return the value
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

// Retrieve a message from the queue and apply the supplied function to the message
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

// Stops consuming and listening and waits for all tasks to finish
func (l *Listener) Close() error {
	if conf.Debug.Verbose {
		log.Println("Listener is waiting for all tasks to finish...")
	}

	// Stop consuming
	go func() {
		l.interrupts <- syscall.SIGINT
	}()

	l.wg.Wait()

	if conf.Debug.Verbose {
		log.Println("Listener is shutting down.")
	}

	return nil
}

func (l *Listener) Errors() <-chan error {
	return l.errors
}

func (l *Listener) Messages() <-chan *Message {
	l.messageMu.Lock()

	defer l.messageMu.Unlock()

	if l.msgBuffer > 0 {
		l.msgBuffer--
	}

	return l.messages
}

func (l *Listener) handleMsg(msg *Message, fn msgFn) {
	if conf.Debug.Verbose {
		if l.getMsgCount()%100 == 0 {
			log.Printf("Messages received: %d", l.getMsgCount())
		}
	}

	defer func() {
		<-l.sem
	}()

	fn(msg.Value(), &l.wg)
}

func (l *Listener) handleInterrupt(signal os.Signal) {
	if conf.Debug.Verbose {
		log.Println("Listener received interrupt signal")
	}

	defer func() {
		<-l.sem
	}()

	l.Close()
}

func (l *Listener) handleError(err error) {
	if err != nil {
		if conf.Debug.Verbose {
			log.Println("Received error: ", err.Error())
		} else if l.errorLogger != nil {
			l.errorLogger.AddError(err)
		}
	}

	defer func() {
		<-l.sem
	}()

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
	if *counter >= kinesisReadsPerSec && !(time.Now().After(timer.Add(1 * time.Second))) {
		// Wait for the remainder of the second - timer and counter will be reset on next pass
		<-time.After(1*time.Second - time.Since(*timer))
	}
}
