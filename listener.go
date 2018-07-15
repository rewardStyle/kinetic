package kinetic

import (
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsKinesis "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

var (
	// ErrNullStream represents an error where the stream was not specified
	ErrNullStream = errors.New("A stream must be specified")
	// ErrNotActive represents an error where the stream is not ready for processing
	ErrNotActive = errors.New("The Stream is not yet active")
)

// Listener represents a kinesis listener
type Listener struct {
	*kinesis

	accessKey string
	secretKey string
	region    string

	concurrency   int
	concurrencyMu sync.Mutex
	sem           chan Empty

	wg sync.WaitGroup

	listening   bool
	listeningMu sync.Mutex
	consuming   bool
	consumingMu sync.Mutex

	errors     chan error
	messages   chan *Message
	interrupts chan os.Signal
}

func (l *Listener) init(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int, endpoint string) (*Listener, error) {
	var err error
	if concurrency < 1 {
		return nil, ErrBadConcurrency
	}
	if stream == "" {
		return nil, ErrNullStream
	}

	l.setConcurrency(concurrency)

	l.accessKey = accessKey
	l.secretKey = secretKey
	l.region = region

	l.sem = make(chan Empty, l.getConcurrency())
	l.errors = make(chan error, l.getConcurrency())
	l.messages = make(chan *Message, l.msgBufSize())

	l.interrupts = make(chan os.Signal, 1)
	signal.Notify(l.interrupts, os.Interrupt)

	l.kinesis, err = new(kinesis).init(stream, shard, shardIterType, accessKey, secretKey, region, endpoint)
	if err != nil {
		return l, err
	}

	// Is the stream ready?
	active, err := l.checkActive()
	if err != nil || !active {
		if err != nil {
			return l, err
		}
		return l, ErrNotActive
	}

	// Start feeder consumer
	go l.consume()

	return l, err
}

// Init initializes a listener with the params specified in the configuration file
func (l *Listener) Init() (*Listener, error) {
	return l.init(conf.Kinesis.Stream, conf.Kinesis.Shard, ShardIterTypes[conf.Kinesis.ShardIteratorType], conf.AWS.AccessKey, conf.AWS.SecretKey, conf.AWS.Region, conf.Concurrency.Listener, "")
}

// InitC initialize a listener with the supplied params
func (l *Listener) InitC(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int) (*Listener, error) {
	return l.init(stream, shard, shardIterType, accessKey, secretKey, region, concurrency, "")
}

// InitC initialize a listener with the supplied params
func (l *Listener) InitCWithEndpoint(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int, endpoint string) (*Listener, error) {
	return l.init(stream, shard, shardIterType, accessKey, secretKey, region, concurrency, endpoint)
}

// NewEndpoint re-initializes kinesis client with new endpoint. Used for testing with kinesalite
func (l *Listener) NewEndpoint(endpoint, stream string) (err error) {
	l.kinesis.client, err = l.kinesis.newClient(endpoint, stream)
	return
}

// ReInit re-initializes the shard iterator.  Used with conjucntion with NewEndpoint
func (l *Listener) ReInit() {
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

// IsListening identifies whether or not messages and errors are being handled after consumption
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

// IsConsuming identifies whether or not the kinesis stream is being polled
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

// Listen handles the consumed messages, errors and interrupts
func (l *Listener) Listen(fn msgFn) {
	l.setListening(true)
stop:
	for {
		getLock(l.sem) //counting semaphore

		select {
		case err := <-l.errors:
			l.incErrCount()
			l.wg.Add(1)
			go l.handleError(err)
		case msg := <-l.messages:
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
		response, err := l.client.GetRecords(
			&awsKinesis.GetRecordsInput{
				Limit:         aws.Int64(10000),
				ShardIterator: aws.String(l.shardIterator),
			},
		)
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

		if response != nil && response.NextShardIterator != nil {
			l.setShardIterator(*response.NextShardIterator)

			if len(response.Records) > 0 {
				for _, record := range response.Records {
					if record != nil {
						l.messages <- &Message{*record}
					}
					l.setSequenceNumber(*record.SequenceNumber)
				}
			}
		}
	}
}

// Retrieve a message from the stream and return the value
func (l *Listener) Retrieve() (*Message, error) {
	select {
	case msg := <-l.messages:
		return msg, nil
	case err := <-l.Errors():
		return nil, err
	case sig := <-l.interrupts:
		l.handleInterrupt(sig)
		return nil, nil
	}
}

// RetrieveFn retrieves a message from the queue and apply the supplied function to the message
func (l *Listener) RetrieveFn(fn msgFn) {
	select {
	case err := <-l.Errors():
		l.wg.Add(1)
		go l.handleError(err)
	case msg := <-l.messages:
		l.wg.Add(1)
		go fn(msg.Value(), &l.wg)
	case sig := <-l.interrupts:
		l.handleInterrupt(sig)
	}
}

// Close stops consuming and listening and waits for all tasks to finish
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
	runtime.Gosched()
	return nil
}

// CloseSync closes the Listener in a syncronous manner.
func (l *Listener) CloseSync() error {
	if conf.Debug.Verbose {
		log.Println("Listener is waiting for all tasks to finish...")
	}
	var err error
	// Stop consuming
	select {
	case l.interrupts <- syscall.SIGINT:
	default:
		if conf.Debug.Verbose {
			log.Println("Already closing listener.")
		}
		runtime.Gosched()
		return err
	}
	l.wg.Wait()
	for l.IsConsuming() {
		runtime.Gosched()
	}

	if conf.Debug.Verbose {
		log.Println("Listener is shutting down.")
	}
	for l.IsListening() {
		runtime.Gosched()
	}
	runtime.Gosched()

	return nil
}

// Errors gets the current number of errors on the Listener
func (l *Listener) Errors() <-chan error {
	return l.errors
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
	if err != nil && conf.Debug.Verbose {
		log.Println("Received error: ", err.Error())
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
		time.Sleep(1*time.Second - time.Since(*timer))
	}
}

func (l *Listener) GetClient() (*kinesisiface.KinesisAPI) {
	return &l.client
}
