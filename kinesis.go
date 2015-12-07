package kinetic

import (
	"sync"
	"time"

	kinesis "github.com/sendgridlabs/go-kinesis"
)

const (
	AtSequenceNumber = iota
	AfterSequenceNumber
	TrimHorizon
	Latest

	StatusCreating = iota
	StatusDeleting
	StatusActive
	StaticUpdating
)

var (
	conf = GetConfig()

	KShardIteratorTypes ShardIteratorTypes = map[int]string{
		AtSequenceNumber:    "AT_SEQUENCE_NUMBER",
		AfterSequenceNumber: "AFTER_SEQUENCE_NUMBER",
		TrimHorizon:         "TRIM_HORIZON",
		Latest:              "LATEST",
	}

	KStreamStatusTypes StreamStatusTypes = map[int]string{
		StatusCreating: "CREATING",
		StatusDeleting: "DELETING",
		StatusActive:   "ACTIVE",
		StaticUpdating: "UPDATING",
	}
)

type msgFn func([]byte, *sync.WaitGroup)
type ShardIteratorTypes map[int]string
type StreamStatusTypes map[int]string

type Kinesis struct {
	stream            string
	shard             string
	shardIteratorType string
	shardIterator     string
	sequenceNumber    string

	client kinesis.KinesisClient
}

func (k *Kinesis) Init(stream, shard, shardIteratorType string) (*Kinesis, error) {
	k = &Kinesis{
		stream:            stream,
		shard:             shard,
		shardIteratorType: shardIteratorType,
		client:            kinesis.New(kinesis.NewAuth(conf.AWS.AccessKey, conf.AWS.SecretKey), conf.AWS.Region),
	}

	err := k.initShardIterator()
	if err != nil {
		return nil, err
	}

	return k, nil
}

func (k *Kinesis) args() *kinesis.RequestArgs {
	args := kinesis.NewArgs()
	args.Add("StreamName", k.stream)
	args.Add("ShardId", k.shard)
	args.Add("ShardIteratorType", k.shardIteratorType)
	args.Add("ShardIterator", k.shardIterator)
	return args
}

func (k *Kinesis) initShardIterator() error {
	resp, err := k.client.GetShardIterator(k.args())
	if err != nil {
		return err
	}

	k.setShardIterator(resp.ShardIterator)
	return nil
}

func (k *Kinesis) setShardIterator(shardIter string) {
	if shardIter == "" || len(shardIter) == 0 {
		return
	}

	k.shardIterator = shardIter
}

func (k *Kinesis) checkActive() (bool, error) {
	status, err := k.client.DescribeStream(k.args())
	if err != nil {
		return false, err
	}

	if KStreamStatusTypes[StatusActive] == status.StreamDescription.StreamStatus {
		return true, nil
	}
	return false, nil
}

func (k *Kinesis) createStream(name string, partitions int) error {
	err := k.client.CreateStream(name, partitions)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kinesis) SetTestEndpoint(endpoint string) {
	// Re-initialize kinesis client for testing
	k = &Kinesis{
		stream:            k.stream,
		shard:             k.shard,
		shardIteratorType: k.shardIteratorType,
		client:            kinesis.NewWithEndpoint(kinesis.NewAuth("BAD_ACCESS_KEY", "BAD_SECRET_KEY"), conf.AWS.Region, endpoint),
	}

	k.createStream(k.stream, 1)

	// Wait for stream to create
	<-time.After(1 * time.Second)

	k.initShardIterator()
}
