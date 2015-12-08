package kinetic

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	gokinesis "github.com/sendgridlabs/go-kinesis"
)

var (
	ThroughputExceededError = errors.New("Configured AWS Kinesis throughput has been exceeded!")
	KinesisFailureError     = errors.New("AWS Kinesis internal failure.")
)

// Producer keeps a queue of messages on a channel and continually attempts
// to POST the records using the PutRecords method. If the messages were
// not sent successfully they are placed back on the queue to retry
type Producer struct {
	*kinesis

	wg          sync.WaitGroup
	msgCount    int
	errCount    int
	producing   bool
	producingMu sync.Mutex

	errors chan error

	// We need to ensure that the sent messages were successfully processed
	// before removing them from this local queue
	messages chan *Message

	interrupts chan os.Signal
}

func (p *Producer) Init(stream, shard string) (*Producer, error) {
	var err error

	if stream == "" {
		return nil, NullStreamError
	}

	p.messages = make(chan *Message)
	p.errors = make(chan error)
	p.interrupts = make(chan os.Signal, 1)

	// Relay incoming interrupt signals to this channel
	signal.Notify(p.interrupts, os.Interrupt)

	p.kinesis, err = new(kinesis).init(stream, shard, shardIterTypes[conf.Kinesis.ShardIteratorType])
	if err != nil {
		return p, err
	}

	// Is the stream ready?
	active, err := p.checkActive()
	if err != nil || active != true {
		if err != nil {
			return p, err
		} else {
			return p, NotActiveError
		}
	}

	// go start feeder consumer and let listen processes them
	go p.produce()

	return p, err
}

func (p *Producer) newEndpoint(endpoint string) {
	// Re-initialize kinesis client for testing
	p.kinesis.client = p.kinesis.newClient(endpoint)
	p.initShardIterator()
}

// Each shard can support up to 1,000 records per second for writes, up to a maximum total
// data write rate of 1 MB per second (including partition keys). This write limit applies
// to operations such as PutRecord and PutRecords.
// http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html
func (p *Producer) flush(counter *int, timer *time.Time) bool {
	// If a second has passed since the last timer start, reset the timer
	if time.Now().After(timer.Add(1 * time.Second)) {
		*timer = time.Now()
		*counter = 0
	}

	*counter++

	// If we have attempted five times and it has been less than one second
	// since we started reading then we need to wait for the second to finish
	if *counter >= 1000 && !(time.Now().After(timer.Add(1 * time.Second))) {
		// Wait for the remainder of the second - timer and counter
		// will be reset on next pass
		<-time.After(1 * time.Second)
	}

	return true
}

func (p *Producer) setProducing(producing bool) {
	p.producingMu.Lock()
	p.producing = producing
	p.producingMu.Unlock()

}

func (p *Producer) IsProducing() bool {
	p.producingMu.Lock()
	defer p.producingMu.Unlock()
	return p.producing
}

// http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
//
// Maximum of 1000 requests a second for a single shard. Each PutRecords can
// accept a maximum of 500 records per request and each record can be as large
// as 1MB per record OR 5MB per request
func (p *Producer) produce() {
	p.setProducing(true)

	counter := 0
	timer := time.Now()

stop:
	for {
		select {
		case msg := <-p.Messages():
			p.msgCount++

			args := p.args()
			args.AddRecord(msg.Value(), string(msg.Key()))

			if p.flush(&counter, &timer) {
				p.wg.Add(1)
				go func() {
					p.SendRecords(args)
					p.wg.Done()
				}()
			}
			break
		case sig := <-p.interrupts:
			go p.handleInterrupt(sig)
			break stop
		case err := <-p.Errors():
			p.errCount++
			p.wg.Add(1)
			go p.handleError(err)
		}
	}

	p.setProducing(false)
}

func (p *Producer) Messages() <-chan *Message {
	return p.messages
}

func (p *Producer) Errors() <-chan error {
	return p.errors
}

// Send a message to the queue for POSTing
func (p *Producer) Send(msg *Message) {
	p.wg.Add(1)
	go func() {
		p.messages <- msg
		p.wg.Done()
	}()
}

// If our payload is larger than allowed Kinesis will write as much as
// possible and fail the rest. We can then put them back on the queue
// to re-send
func (p *Producer) SendRecords(args *gokinesis.RequestArgs) {
	putResp, err := p.client.PutRecords(args)
	if err != nil && conf.Debug.Verbose {
		p.errors <- err
	}

	// Because we do not know which of the records was successful or failed
	// we need to put them all back on the queue
	if putResp != nil && putResp.FailedRecordCount > 0 {
		if conf.Debug.Verbose {
			log.Println("Failed records: " + strconv.Itoa(putResp.FailedRecordCount))
		}

		for idx, resp := range putResp.Records {
			// Put failed records back on the queue
			if resp.ErrorCode != "" || resp.ErrorMessage != "" {
				p.msgCount--
				p.errors <- errors.New(resp.ErrorMessage)
				p.Send(new(Message).Init(args.Records[idx].Data, args.Records[idx].PartitionKey))

				if conf.Debug.Verbose {
					log.Println("Messages in failed PutRecords put back on the queue: " + string(args.Records[idx].Data))
				}
			} else {
				if conf.Debug.Verbose {
					log.Println("Successfully sent all messages.")
				}
			}

		}
	}
}

func (p *Producer) Close() error {
	p.wg.Wait()

	// Stop producing
	go func() {
		p.interrupts <- syscall.SIGINT
	}()

	return nil
}

func (p *Producer) handleInterrupt(signal os.Signal) {
	if conf.Debug.Verbose {
		log.Println("Received interrupt signal")
	}

	p.Close()
}

func (p *Producer) handleError(err error) {
	if err != nil && conf.Debug.Verbose {
		log.Println("Received error: ", err.Error())
	}

	p.wg.Done()
}
