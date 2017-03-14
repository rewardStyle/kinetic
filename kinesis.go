package kinetic

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	gokinesis "github.com/rewardStyle/go-kinesis"
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
	kinesisReadsPerSec  int = 5
	// Timeout TODO
	Timeout = 60
)

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
	shardIteratorType string
	shardIterator     string
	sequenceNumber    string
	sequenceNumberMu  sync.Mutex

	client gokinesis.KinesisClient

	msgCount int64
	errCount int64
}

func (k *kinesis) init(stream, shard, shardIteratorType, accessKey, secretKey, region string) (*kinesis, error) {

	auth, err := authenticate(accessKey, secretKey)
	k = &kinesis{
		stream:            stream,
		shard:             shard,
		shardIteratorType: shardIteratorType,
		client:            gokinesis.New(auth, region),
	}
	if err != nil {
		return k, err
	}

	err = k.initShardIterator()
	if err != nil {
		return k, err
	}

	return k, nil
}

func (k *kinesis) args() *gokinesis.RequestArgs {
	args := gokinesis.NewArgs()
	args.Add("StreamName", k.stream)
	args.Add("ShardId", k.shard)
	args.Add("ShardIterator", k.shardIterator)

	if k.sequenceNumber != "" {
		args.Add("StartingSequenceNumber", k.sequenceNumber)
		args.Add("ShardIteratorType", ShardIterTypes[atSequenceNumber])
	} else {
		args.Add("ShardIteratorType", k.shardIteratorType)
	}

	return args
}

func (k *kinesis) initShardIterator() error {
	resp, err := k.client.GetShardIterator(k.args())
	if err != nil {
		return err
	}

	return k.setShardIterator(resp.ShardIterator)
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
	status, err := k.client.DescribeStream(k.args())
	if err != nil {
		return false, err
	}

	if streamStatuses[statusActive] == status.StreamDescription.StreamStatus {
		return true, nil
	}
	return false, nil
}

func (k *kinesis) newClient(endpoint, stream string) gokinesis.KinesisClient {
	client := gokinesis.NewWithEndpoint(gokinesis.NewAuth("BAD_ACCESS_KEY", "BAD_SECRET_KEY", "BAD_TOKEN"), conf.AWS.Region, endpoint)
	client.CreateStream(stream, 1)

	// Wait for stream to create
	<-time.After(1 * time.Second)

	return client
}

func (k *kinesis) refreshClient(accessKey, secretKey, region string) error {
	credentials, err := authenticate(accessKey, secretKey)
	if err != nil {
		return err
	}
	k.client = gokinesis.New(credentials, region)
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
