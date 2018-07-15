package kinetic

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	awsKinesis "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	atSequenceNumber = iota
	afterSequenceNumber
	trimHorizon
	latest

	statusCreating = iota
	statusDeleting
	statusActive
	staticUpdating

	kinesisWritesPerSec int = 1000
	kinesisReadsPerSec  int = 2
	// Timeout TODO
)

var (
	// HTTPTimeout is the timeout for http for requests.
	HTTPTimeout = 60 * time.Second
	// DialTimeout is the timeout for the tpc dial.
	DialTimeout = 5 * time.Second
	// HandShakeTimeout is the timeout for the tpc handshake.
	HandShakeTimeout = 5 * time.Second
)

// ErrNilShardIterator is an error for when we get back a nil shard iterator
var ErrNilShardIterator = errors.New("Nil shard iterator")

// ErrNilShardStatus is an error for when we get back a nil stream status
var ErrNilShardStatus = errors.New("Nil stream status")

// Empty is an empty struct.  It is mostly used for counting semaphore purposes
type Empty struct{}

var (
	conf = getConfig()

	// ShardIterTypes are the types of iterators to use within Kinesis
	ShardIterTypes shardIteratorTypes = map[int]string{
		atSequenceNumber:    "AT_SEQUENCE_NUMBER",
		afterSequenceNumber: "AFTER_SEQUENCE_NUMBER",
		trimHorizon:         "TRIM_HORIZON",
		latest:              "LATEST",
	}

	streamStatuses streamStatusTypes = map[int]string{
		statusCreating: "CREATING",
		statusDeleting: "DELETING",
		statusActive:   "ACTIVE",
		staticUpdating: "UPDATING",
	}
)

type msgFn func([]byte, *sync.WaitGroup)
type shardIteratorTypes map[int]string
type streamStatusTypes map[int]string

type kinesis struct {
	stream            string
	shard             string
	endPoint          string
	shardIteratorType string
	shardIterator     string
	sequenceNumber    string
	sequenceNumberMu  sync.Mutex

	client kinesisiface.KinesisAPI

	msgCount int64
	errCount int64
}

func (k *kinesis) init(stream, shard, shardIteratorType, accessKey, secretKey, region, endpoint string) (*kinesis, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: DialTimeout,
			}).Dial,
			TLSHandshakeTimeout: HandShakeTimeout},
		Timeout: HTTPTimeout,
	}
	sess, err := authenticate(accessKey, secretKey)
	awsConf := aws.NewConfig().WithRegion(region).WithHTTPClient(httpClient)
	if conf.Debug.Verbose {
		awsConf = awsConf.WithLogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors)
	}
	if endpoint != "" {
		awsConf = awsConf.WithEndpoint(endpoint)
	}
	client := awsKinesis.New(sess, awsConf)

	k = &kinesis{
		stream:            stream,
		shard:             shard,
		shardIteratorType: shardIteratorType,
		client:            client,
	}
	if err != nil {
		return k, err
	}

	err = k.initShardIterator()
	return k, err
}

func (k *kinesis) initShardIterator() error {
	var awsSeqNumber *string
	if k.sequenceNumber != "" {
		awsSeqNumber = aws.String(k.sequenceNumber)
		k.shardIteratorType = ShardIterTypes[atSequenceNumber]
	}
	resp, err := k.client.GetShardIterator(&awsKinesis.GetShardIteratorInput{
		ShardId:                aws.String(k.shard),             // Required
		ShardIteratorType:      aws.String(k.shardIteratorType), // Required
		StreamName:             aws.String(k.stream),            // Required
		StartingSequenceNumber: awsSeqNumber,
	})
	if err != nil {
		return err
	}
	if resp.ShardIterator != nil {
		return k.setShardIterator(*resp.ShardIterator)
	}

	return ErrNilShardIterator
}

func (k *kinesis) setSequenceNumber(sequenceNum string) {
	if sequenceNum == "" || len(sequenceNum) == 0 {
		return
	}

	k.sequenceNumberMu.Lock()
	k.sequenceNumber = sequenceNum
	k.sequenceNumberMu.Unlock()
}

func (k *kinesis) setShardIterator(shardIter string) error {
	if shardIter == "" || len(shardIter) == 0 {
		return errors.New("Attempted to set shard iterator with empty value")
	}

	k.shardIterator = shardIter

	return nil
}

func (k *kinesis) checkActive() (bool, error) {
	status, err := k.client.DescribeStream(&awsKinesis.DescribeStreamInput{
		StreamName: aws.String(k.stream), // Required
	})
	if err != nil {
		return false, err
	}
	if status.StreamDescription.StreamStatus == nil {
		return false, ErrNilShardStatus
	}
	if streamStatuses[statusActive] == *status.StreamDescription.StreamStatus {
		return true, nil
	}
	return false, nil
}

func (k *kinesis) refreshClient(accessKey, secretKey, region string) error {
	sess, err := authenticate(accessKey, secretKey)
	conf := aws.NewConfig().WithRegion(region).WithEndpoint(k.endPoint)
	if err != nil {
		return err
	}
	k.client = awsKinesis.New(sess, conf)
	return nil
}

func (k *kinesis) decMsgCount() {
	atomic.AddInt64(&k.msgCount, -1)
}

func (k *kinesis) incMsgCount() {
	atomic.AddInt64(&k.msgCount, 1)
}

func (k *kinesis) getMsgCount() int64 {
	return atomic.LoadInt64(&k.msgCount)
}

func (k *kinesis) decErrCount() {
	atomic.AddInt64(&k.errCount, -1)
}

func (k *kinesis) incErrCount() {
	atomic.AddInt64(&k.errCount, 1)
}

func (k *kinesis) getErrCount() int64 {
	return atomic.LoadInt64(&k.errCount)
}

func getLock(sem chan Empty) {
	sem <- Empty{}
}
