package kinetic

//gokinesis "github.com/rewardStyle/go-kinesis"
import (
	"errors"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsFirehose "github.com/aws/aws-sdk-go/service/firehose"
	awsFirehoseIface "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
)

const (
	firehoseWritesPerSec      int64  = 2000
	truncatedRecordTerminator string = ``
)

// Firehose is a Producer
type Firehose struct {
	stream        string
	client        awsFirehoseIface.FirehoseAPI
	msgCount      int64
	errCount      int64
	concurrency   int
	concurrencyMu sync.Mutex
	sem           chan Empty
	wg            sync.WaitGroup
	producing     bool
	producingMu   sync.Mutex
	errors        chan error
	messages      chan *Message
	interrupts    chan os.Signal
}

func (p *Firehose) activate() (Producer, error) {
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

// Close stops queuing and producing and waits for all tasks to finish
func (p *Firehose) Close() error {
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

// CloseSync closes the Firehose producer in a syncronous manner.
func (p *Firehose) CloseSync() error {
	if conf.Debug.Verbose {
		log.Println("Producer is waiting for all tasks to finish...")
	}

	select {
	case p.interrupts <- syscall.SIGINT:
		break
	default:
		if conf.Debug.Verbose {
			log.Println("Already closing listener.")
		}
		runtime.Gosched()
		return nil
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

func (p *Firehose) initChannels() {
	p.sem = make(chan Empty, p.getConcurrency())
	p.errors = make(chan error, p.getConcurrency())
	p.messages = make(chan *Message, p.msgBufSize())

	p.interrupts = make(chan os.Signal, 1)
	signal.Notify(p.interrupts, os.Interrupt)
}

func (p *Firehose) checkActive() (bool, error) {
	status, err := p.client.DescribeDeliveryStream(
		&awsFirehose.DescribeDeliveryStreamInput{
			DeliveryStreamName: aws.String(p.stream)})
	if err != nil {
		return false, err
	}

	if streamStatuses[statusActive] == aws.StringValue(status.DeliveryStreamDescription.DeliveryStreamStatus) {
		return true, nil
	}
	return false, nil
}

func (p *Firehose) setConcurrency(concurrency int) {
	p.concurrencyMu.Lock()
	p.concurrency = concurrency
	p.concurrencyMu.Unlock()
}

func (p *Firehose) getConcurrency() (concurrency int) {
	p.concurrencyMu.Lock()
	concurrency = p.concurrency
	p.concurrencyMu.Unlock()
	return
}

func (p *Firehose) msgBufSize() int {
	return p.getConcurrency() * 1000
}

// Send a message to Firehose asyncronously
func (p *Firehose) Send(msg *Message) {
	msg.SetValue(append(msg.Value(), truncatedRecordTerminator...))
	p.wg.Add(1)
	go func() {
		p.messages <- msg
		p.wg.Done()
	}()
}

// SendSync - blocking send.
// TODO: we might want a version of this that can be fed a timeout value
// and listen for cancellation channels. This version is for simplicity.
func (p *Firehose) SendSync(msg *Message) {
	msg.SetValue(append(msg.Value(), truncatedRecordTerminator...))
	p.wg.Add(1)
	p.messages <- msg
	p.wg.Done()
}

// TryToSend tries to send the message, but if the channel is full it drops the message, and returns an error.
func (p *Firehose) TryToSend(msg *Message) error {
	msg.SetValue(append(msg.Value(), truncatedRecordTerminator...))
	p.wg.Add(1)
	select {
	case p.messages <- msg:
		return nil
	default:
		return ErrDroppedMessage
	}
}

// Init initalizes a firehose producer with the config file defaults
func (p *Firehose) Init() (Producer, error) {
	if conf.Concurrency.Producer < 1 {
		return nil, ErrBadConcurrency
	}
	p.setConcurrency(conf.Concurrency.Producer)
	p.initChannels()
	sess, err := authenticate(conf.AWS.AccessKey, conf.AWS.SecretKey)
	p.stream = conf.Firehose.Stream
	p.client = awsFirehose.New(sess)
	//gokinesis.NewWithEndpoint(auth, conf.AWS.Region, fmt.Sprintf(firehoseURL, conf.AWS.Region)),

	if err != nil {
		return p, err
	}

	return p.activate()
}

// InitC initializes a producer for Kinesis Firehose with the specified params
func (p *Firehose) InitC(stream, _, _, accessKey, secretKey, region string, concurrency int) (Producer, error) {
	if concurrency < 1 {
		return nil, ErrBadConcurrency
	}
	if stream == "" {
		return nil, ErrNullStream
	}

	p.setConcurrency(concurrency)
	p.initChannels()
	sess, err := authenticate(accessKey, secretKey)
	conf := &aws.Config{Region: aws.String(region)}
	p.stream = stream
	p.client = awsFirehose.New(sess, conf)
	if err != nil {
		return p, err
	}

	return p.activate()
}

// InitCWithEndpoint initializes a producer for Kinesis Firehose with the specified params
func (p *Firehose) InitCWithEndpoint(stream, _, _, accessKey, secretKey, region string, concurrency int, endpoint string) (Producer, error) {
	if concurrency < 1 {
		return nil, ErrBadConcurrency
	}
	if stream == "" {
		return nil, ErrNullStream
	}

	p.setConcurrency(concurrency)
	p.initChannels()
	sess, err := authenticate(accessKey, secretKey)
	conf := &aws.Config{Region: aws.String(region)}
	if endpoint != "" {
		conf = conf.WithEndpoint(endpoint)
	}
	p.stream = stream
	p.client = awsFirehose.New(sess, conf)
	if err != nil {
		return p, err
	}

	return p.activate()
}

// IsProducing returns true if Firehose is producing otherwise false
func (p *Firehose) IsProducing() (isProducing bool) {
	p.producingMu.Lock()
	isProducing = p.producing
	p.producingMu.Unlock()
	return
}

func (p *Firehose) setProducing(producing bool) {
	p.producingMu.Lock()
	p.producing = producing
	p.producingMu.Unlock()
}

// NewEndpoint switches the endpoint of the firehose stream.  This is useful for testing.
func (p *Firehose) NewEndpoint(endpoint, stream string) (err error) {
	conf := &aws.Config{}
	conf = conf.WithCredentials(
		credentials.NewStaticCredentials("BAD_ACCESS_KEY", "BAD_SECRET_KEY", "BAD_TOKEN"),
	).WithEndpoint(endpoint).WithRegion("us-east-1")
	sess, err := session.NewSessionWithOptions(session.Options{Config: *conf})
	p.client = awsFirehose.New(sess)
	return err
}

// Messages gets the current message channel from the producer
func (p *Firehose) Messages() <-chan *Message {
	return p.messages
}

// Errors gets the current number of errors on the Producer
func (p *Firehose) Errors() <-chan error {
	return p.errors
}

func (p *Firehose) produce() {
	p.setProducing(true)
	var counter int64
stop:
	for {
		getLock(p.sem)
		select {
		case msg := <-p.Messages():
			timer := time.Now()
			if p.firehoseFlush(&counter, &timer) {
				p.wg.Add(1)
				go func() {
					records := []*awsFirehose.Record{&awsFirehose.Record{Data: msg.Value()}}
					if conf.Debug.Verbose && atomic.LoadInt64(&counter)%100 == 0 {
						log.Println("Attempting to send firehose messages")
					}
					p.sendFirehoseRecords(records...)
					p.wg.Done()
				}()
			}
			<-p.sem
		case <-p.interrupts:
			if conf.Debug.Verbose {
				log.Println("Producer received interrupt signal")
			}
			<-p.sem
			p.Close()
			break stop
		case err := <-p.Errors():
			if err != nil && conf.Debug.Verbose {
				log.Println("Received error: ", err.Error())
			}
			p.incErrCount()
			<-p.sem
		}
	}
}

// Each firehose stream can support up to 2,000 transactions per second for writes,
// 5,000 records a second, up to a maximum total data write rate of 5 MB per second by default.
// TODO: payload inspection & throttling
// PutRecordBatch can take up to 500 records per call or 4 MB per call, whichever is smaller.
// http://docs.aws.amazon.com/firehose/latest/dev/limits.html
func (p *Firehose) firehoseFlush(counter *int64, timer *time.Time) bool {
	// If a second has passed since the last timer start, reset the timer
	if time.Now().After(timer.Add(1 * time.Second)) {
		*timer = time.Now()
		atomic.StoreInt64(counter, 0)
	}

	atomic.AddInt64(counter, 1)

	// If we have attempted 5000 times and it has been less than one second
	// since we started sending then we need to wait for the second to finish
	if atomic.LoadInt64(counter) >= firehoseWritesPerSec && !(time.Now().After(timer.Add(1 * time.Second))) {
		// Wait for the remainder of the second - timer and counter
		// will be reset on next pass
		time.Sleep(time.Since(*timer))
	}

	return true
}

// Queue the messages sent to firehose for POSTing.
// msgNum is number of the messge till now.  It is just for only logging every 100 messages.
// if you want it to always
func (p *Firehose) sendFirehoseRecords(records ...*awsFirehose.Record) {
	putResp, err := p.client.PutRecordBatch(&awsFirehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(p.stream),
		Records:            records,
	})
	if err != nil && conf.Debug.Verbose {
		p.errors <- err
	}

	// Because we do not know which of the records was successful or failed
	// we need to put them all back on the queue
	if putResp != nil {
		if aws.Int64Value(putResp.FailedPutCount) > 0 {
			if conf.Debug.Verbose {
				log.Printf("Failed firehose records: %d\n", aws.Int64Value(putResp.FailedPutCount))
			}

			for idx, resp := range putResp.RequestResponses {
				// Put failed records back on the queue
				if aws.StringValue(resp.ErrorCode) != "" || aws.StringValue(resp.ErrorMessage) != "" {
					p.errors <- errors.New(aws.StringValue(resp.ErrorMessage))
					p.Send(new(Message).Init(records[idx].Data, ""))

					if conf.Debug.Verbose {
						log.Println("Messages in failed PutRecords put back on the queue: " + string(records[idx].Data))
					}
				} else {
					p.incMsgCount()
				}
			}
		} else {
			p.incMsgCountBy(len(putResp.RequestResponses))
		}
	} else if putResp == nil {
		//resend records when we get a nil response
		for _, record := range records {
			p.Send(new(Message).Init(record.Data, ""))
			if conf.Debug.Verbose {
				log.Println("Message in nil send response put back on the queue: " + string(record.Data))
			}
		}
	}

	if conf.Debug.Verbose && p.getMsgCount()%100 == 0 {
		log.Printf("Messages sent so far: %d\n", p.getMsgCount())
	}
}

// ReInit re-initializes the shard iterator.  Used with conjucntion with NewEndpoint
func (p *Firehose) ReInit() {
	if !p.IsProducing() {
		go p.produce()
	}
}

func (p *Firehose) incMsgCount() {
	atomic.AddInt64(&p.msgCount, 1)
}

func (p *Firehose) incMsgCountBy(n int) {
	atomic.AddInt64(&p.msgCount, int64(n))
}

func (p *Firehose) getMsgCount() int64 {
	return atomic.LoadInt64(&p.msgCount)
}

func (p *Firehose) decErrCount() {
	atomic.AddInt64(&p.errCount, -1)
}

func (p *Firehose) incErrCount() {
	atomic.AddInt64(&p.errCount, 1)
}

func (p *Firehose) getErrCount() int64 {
	return atomic.LoadInt64(&p.errCount)
}
