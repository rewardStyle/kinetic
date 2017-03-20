package kinetic

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsKinesis "github.com/aws/aws-sdk-go/service/kinesis"
)

var _ = awsKinesis.EndpointsID

var (
	// ErrThroughputExceeded represents an error when the Kinesis throughput has been exceeded
	ErrThroughputExceeded = errors.New("Configured AWS Kinesis throughput has been exceeded")
	// ErrKinesisFailure represents a generic internal AWS Kinesis error
	ErrKinesisFailure = errors.New("AWS Kinesis internal failure")
	// ErrBadConcurrency represents an error when the provided concurrency value is invalid
	ErrBadConcurrency = errors.New("Concurrency must be greater than zero")
	// ErrDroppedMessage represents an error the channel is full and messages are being dropped
	ErrDroppedMessage = errors.New("Channel is full, dropped message")
)

// Producer is an interface for sending messages to a stream of some sort.
type Producer interface {
	Init() (Producer, error)
	InitC(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int) (Producer, error)
	NewEndpoint(endpoint, stream string) (err error)
	ReInit()
	IsProducing() bool
	Send(msg *Message)
	TryToSend(msg *Message) error
	Close() error
	CloseSync() error
}

// KinesisProducer keeps a queue of messages on a channel and continually attempts
// to POST the records using the PutRecords method. If the messages were
// not sent successfully they are placed back on the queue to retry
type KinesisProducer struct {
	*kinesis

	concurrency   int
	concurrencyMu sync.Mutex
	sem           chan Empty

	wg sync.WaitGroup

	producing   bool
	producingMu sync.Mutex

	errors chan error

	// We need to ensure that the sent messages were successfully processed
	// before removing them from this local queue
	messages   chan *Message
	interrupts chan os.Signal
}

func (p *KinesisProducer) init(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int) (Producer, error) {
	var err error
	if concurrency < 1 {
		return nil, ErrBadConcurrency
	}
	if stream == "" {
		return nil, ErrNullStream
	}

	p.setConcurrency(concurrency)

	p.initChannels()

	p.kinesis, err = new(kinesis).init(stream, shard, shardIterType, accessKey, secretKey, region)
	if err != nil {
		return p, err
	}

	return p.activate()
}

func (p *KinesisProducer) initChannels() {
	p.sem = make(chan Empty, p.getConcurrency())
	p.errors = make(chan error, p.getConcurrency())
	p.messages = make(chan *Message, p.msgBufSize())

	p.interrupts = make(chan os.Signal, 1)
	signal.Notify(p.interrupts, os.Interrupt)
}

func (p *KinesisProducer) setConcurrency(concurrency int) {
	p.concurrencyMu.Lock()
	p.concurrency = concurrency
	p.concurrencyMu.Unlock()
}

func (p *KinesisProducer) getConcurrency() int {
	p.concurrencyMu.Lock()
	defer p.concurrencyMu.Unlock()
	return p.concurrency
}

func (p *KinesisProducer) msgBufSize() int {
	p.concurrencyMu.Lock()
	defer p.concurrencyMu.Unlock()
	return p.concurrency * 1000
}

func (p *KinesisProducer) activate() (Producer, error) {
	// Is the stream ready?
	active, err := p.checkActive()
	if err != nil || !active {
		if err != nil {
			return p, err
		}
		return p, ErrNotActive
	}

	// go start feeder consumer and let listen processes them
	go p.produce()

	return p, err
}

// Init initializes a producer with the params specified in the configuration file
func (p *KinesisProducer) Init() (Producer, error) {
	return p.init(conf.Kinesis.Stream, conf.Kinesis.Shard, ShardIterTypes[conf.Kinesis.ShardIteratorType], conf.AWS.AccessKey, conf.AWS.SecretKey, conf.AWS.Region, conf.Concurrency.Producer)
}

// InitC initializes a producer with the specified configuration: stream, shard, shard-iter-type, access-key, secret-key, and region
func (p *KinesisProducer) InitC(stream, shard, shardIterType, accessKey, secretKey, region string, concurrency int) (Producer, error) {
	return p.init(stream, shard, shardIterType, accessKey, secretKey, region, concurrency)
}

// NewEndpoint re-initializes kinesis client with new endpoint. Used for testing with kinesalite
func (p *KinesisProducer) NewEndpoint(endpoint, stream string) (err error) {
	// Re-initialize kinesis client for testing
	p.kinesis.client, err = p.kinesis.newClient(endpoint, stream)
	return
}

// ReInit re-initializes the shard iterator.  Used with conjucntion with NewEndpoint
func (p *KinesisProducer) ReInit() {
	if !p.IsProducing() {
		go p.produce()
	}
	return
}

// Each shard can support up to 1,000 records per second for writes, up to a maximum total
// data write rate of 1 MB per second (including partition keys). This write limit applies
// to operations such as PutRecord and PutRecords.
// TODO: payload inspection & throttling
// http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html
func (p *KinesisProducer) kinesisFlush(counter *int, timer *time.Time) bool {
	// If a second has passed since the last timer start, reset the timer
	if time.Now().After(timer.Add(1 * time.Second)) {
		*timer = time.Now()
		*counter = 0
	}

	*counter++

	// If we have attempted 1000 times and it has been less than one second
	// since we started sending then we need to wait for the second to finish
	if *counter >= kinesisWritesPerSec && !(time.Now().After(timer.Add(1 * time.Second))) {
		// Wait for the remainder of the second - timer and counter
		// will be reset on next pass
		time.Sleep(1*time.Second - time.Since(*timer))
	}

	return true
}

func (p *KinesisProducer) setProducing(producing bool) {
	p.producingMu.Lock()
	p.producing = producing
	p.producingMu.Unlock()

}

// IsProducing identifies whether or not the messages are queued for POSTing to the stream
func (p *KinesisProducer) IsProducing() bool {
	p.producingMu.Lock()
	defer p.producingMu.Unlock()
	return p.producing
}

// http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
//
// Maximum of 1000 requests a second for a single shard. Each PutRecords can
// accept a maximum of 500 records per request and each record can be as large
// as 1MB per record OR 5MB per request
func (p *KinesisProducer) produce() {
	p.setProducing(true)

	counter := 0
	timer := time.Now()

stop:
	for {
		getLock(p.sem)

		select {
		case msg := <-p.Messages():
			p.incMsgCount()

			if conf.Debug.Verbose && p.getMsgCount()%100 == 0 {
				log.Println("Received message to send. Total messages received: " + strconv.FormatInt(p.getMsgCount(), 10))
			}
			kargs := &awsKinesis.PutRecordsInput{StreamName: aws.String(p.stream)}
			kargs.Records = append(
				kargs.Records,
				&awsKinesis.PutRecordsRequestEntry{
					Data:         msg.Value(),
					PartitionKey: aws.String(string(msg.Key()))})
			if p.kinesisFlush(&counter, &timer) {
				p.wg.Add(1)
				go func() {
					p.sendRecords(kargs)
					p.wg.Done()
				}()
			}

			<-p.sem
		case sig := <-p.interrupts:
			go p.handleInterrupt(sig)
			break stop
		case err := <-p.Errors():
			p.incErrCount()
			p.wg.Add(1)
			go p.handleError(err)
		}
	}

	p.setProducing(false)
}

// Messages gets the current message channel from the producer
func (p *KinesisProducer) Messages() <-chan *Message {
	return p.messages
}

// Errors gets the current number of errors on the Producer
func (p *KinesisProducer) Errors() <-chan error {
	return p.errors
}

// Send a message to the queue for POSTing
func (p *KinesisProducer) Send(msg *Message) {
	// Add the terminating record indicator
	p.wg.Add(1)
	go func() {
		p.messages <- msg
		p.wg.Done()
	}()
}

// TryToSend tries to send the message, but if the channel is full it drops the message, and returns an error.
func (p *KinesisProducer) TryToSend(msg *Message) error {
	// Add the terminating record indicator
	select {
	case p.messages <- msg:
		return nil
	default:
		return ErrDroppedMessage
	}
}

// If our payload is larger than allowed Kinesis will write as much as
// possible and fail the rest. We can then put them back on the queue
// to re-send
func (p *KinesisProducer) sendRecords(args *awsKinesis.PutRecordsInput) {

	putResp, err := p.client.PutRecords(args)
	if err != nil && conf.Debug.Verbose {
		p.errors <- err
	}

	// Because we do not know which of the records was successful or failed
	// we need to put them all back on the queue
	failedRecordCount := aws.Int64Value(putResp.FailedRecordCount)
	if putResp != nil && failedRecordCount > 0 {
		if conf.Debug.Verbose {
			log.Printf("Failed records: %d", failedRecordCount)
		}

		for idx, resp := range putResp.Records {
			// Put failed records back on the queue
			errorCode := aws.StringValue(resp.ErrorCode)
			errorMessage := aws.StringValue(resp.ErrorMessage)
			if errorCode != "" || errorMessage != "" {
				p.decMsgCount()
				p.errors <- errors.New(errorMessage)
				p.Send(new(Message).Init(args.Records[idx].Data, aws.StringValue(args.Records[idx].PartitionKey)))

				if conf.Debug.Verbose {
					log.Println("Message in failed PutRecords put back on the queue: " + string(args.Records[idx].Data))
				}
			}
		}
	} else if putResp == nil {
		// Retry posting these records as they most likely were not posted successfully
		p.retryRecords(args.Records)
	}

	if conf.Debug.Verbose && p.getMsgCount()%100 == 0 {
		log.Println("Messages sent so far: " + strconv.FormatInt(p.getMsgCount(), 10))
	}
}

func (p *KinesisProducer) retryRecords(records []*awsKinesis.PutRecordsRequestEntry) {
	for _, record := range records {
		p.Send(new(Message).Init(record.Data, aws.StringValue(record.PartitionKey)))

		if conf.Debug.Verbose {
			log.Println("Message in nil send response put back on the queue: " + string(record.Data))
		}
	}
}

// Close stops queuing and producing and waits for all tasks to finish
func (p *KinesisProducer) Close() error {
	if conf.Debug.Verbose {
		log.Println("Producer is waiting for all tasks to finish...")
	}

	p.wg.Wait()

	// Stop producing
	go func() {
		p.interrupts <- syscall.SIGINT
	}()

	if conf.Debug.Verbose {
		log.Println("Producer is shutting down.")
	}
	runtime.Gosched()
	return nil
}

// CloseSync closes the Producer in a syncronous manner.
func (p *KinesisProducer) CloseSync() error {
	if conf.Debug.Verbose {
		log.Println("Listener is waiting for all tasks to finish...")
	}
	var err error
	// Stop consuming
	select {
	case p.interrupts <- syscall.SIGINT:
		break
	default:
		if conf.Debug.Verbose {
			log.Println("Already closing listener.")
		}
		runtime.Gosched()
		return err
	}
	p.wg.Wait()
	for p.IsProducing() {
		runtime.Gosched()
	}
	if conf.Debug.Verbose {
		log.Println("Listener is shutting down.")
	}
	runtime.Gosched()
	return nil
}

func (p *KinesisProducer) handleInterrupt(signal os.Signal) {
	if conf.Debug.Verbose {
		log.Println("Producer received interrupt signal")
	}

	defer func() {
		<-p.sem
	}()

	p.Close()
}

func (p *KinesisProducer) handleError(err error) {
	if err != nil && conf.Debug.Verbose {
		log.Println("Received error: ", err.Error())
	}

	defer func() {
		<-p.sem
	}()

	p.wg.Done()
}
