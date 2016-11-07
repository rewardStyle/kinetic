package kinetic

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	gokinesis "github.com/rewardStyle/go-kinesis"
)

const (
	firehoseWritesPerSec      int    = 2000
	truncatedRecordTerminator string = ``
)

func (k *kinesis) firehoseArgs() *gokinesis.RequestArgs {
	args := gokinesis.NewArgs()
	args.Add("DeliveryStreamName", k.stream)
	return args
}

func (k *kinesis) checkFirehoseActive() (bool, error) {
	status, err := k.client.DescribeDeliveryStream(k.firehoseArgs())
	if err != nil {
		return false, err
	}

	if streamStatuses[statusActive] == status.DeliveryStreamDescription.DeliveryStreamStatus {
		return true, nil
	}
	return false, nil
}

// Each firehose stream can support up to 2,000 transactions per second for writes,
// 5,000 records a second, up to a maximum total data write rate of 5 MB per second by default.
// TODO: payload inspection & throttling
// PutRecordBatch can take up to 500 records per call or 4 MB per call, whichever is smaller.
// http://docs.aws.amazon.com/firehose/latest/dev/limits.html
func (p *Producer) firehoseFlush(counter *int, timer *time.Time) bool {
	// If a second has passed since the last timer start, reset the timer
	if time.Now().After(timer.Add(1 * time.Second)) {
		*timer = time.Now()
		*counter = 0
	}

	*counter++

	// If we have attempted 1000 times and it has been less than one second
	// since we started sending then we need to wait for the second to finish
	if *counter >= firehoseWritesPerSec && !(time.Now().After(timer.Add(1 * time.Second))) {
		// Wait for the remainder of the second - timer and counter
		// will be reset on next pass
		<-time.After(1 * time.Second)
	}

	return true
}

// Initialize a producer for Kinesis Firehose with the params supplied in the configuration file
func (p *Producer) Firehose() (*Producer, error) {
	p.setConcurrency(conf.Concurrency.Producer)
	p.initChannels()
	auth, err := authenticate(conf.AWS.AccessKey, conf.AWS.SecretKey)
	p.kinesis = &kinesis{
		stream: conf.Firehose.Stream,
		client: gokinesis.NewWithEndpoint(auth, conf.AWS.Region, fmt.Sprintf(firehoseURL, conf.AWS.Region)),
	}
	if err != nil {
		return p, err
	}

	p.producerType = firehoseType

	return p.activate()
}

// Initialize a producer for Kinesis Firehose with the specified params
func (p *Producer) FirehoseC(stream, accessKey, secretKey, region string, concurrency int) (*Producer, error) {
	if stream == "" {
		return nil, NullStreamError
	}

	p.setConcurrency(concurrency)
	p.initChannels()
	auth, err := authenticate(accessKey, secretKey)
	p.kinesis = &kinesis{
		stream: stream,
		client: gokinesis.NewWithEndpoint(auth, region, fmt.Sprintf(firehoseURL, region)),
	}
	if err != nil {
		return p, err
	}

	p.producerType = firehoseType

	return p.activate()
}

// Queue the messages sent to firehose for POSTing
func (p *Producer) sendFirehoseRecords(args *gokinesis.RequestArgs) {
	if p.getProducerType() != firehoseType {
		return
	}

	putResp, err := p.client.PutRecordBatch(args)
	if err != nil && conf.Debug.Verbose {
		p.errors <- err
	}

	if conf.Debug.Verbose && p.getMsgCount()%100 == 0 {
		log.Println("Attempting to send firehose messages")
	}

	// Because we do not know which of the records was successful or failed
	// we need to put them all back on the queue
	if putResp != nil && putResp.FailedPutCount > 0 {
		if conf.Debug.Verbose {
			log.Println("Failed firehose records: " + strconv.Itoa(putResp.FailedPutCount))
		}

		for idx, resp := range putResp.RequestResponses {
			// Put failed records back on the queue
			if resp.ErrorCode != "" || resp.ErrorMessage != "" {
				p.decMsgCount()
				p.errors <- errors.New(resp.ErrorMessage)
				p.Send(new(Message).Init(args.Records[idx].Data, args.Records[idx].PartitionKey))

				if conf.Debug.Verbose {
					log.Println("Messages in failed PutRecords put back on the queue: " + string(args.Records[idx].Data))
				}
			}
		}
	} else if putResp == nil {
		p.retryRecords(args.Records)
	}

	if conf.Debug.Verbose && p.getMsgCount()%100 == 0 {
		log.Println("Messages sent so far: " + strconv.Itoa(p.getMsgCount()))
	}
}
