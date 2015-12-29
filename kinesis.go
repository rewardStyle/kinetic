package kinetic

import (
	"sync"
	"time"

	gokinesis "github.com/sendgridlabs/go-kinesis"
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
)

var (
	conf = getConfig()

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

	client gokinesis.KinesisClient
}

func (k *kinesis) init(stream, shard, shardIteratorType, accessKey, secretKey, region string) (*kinesis, error) {
	k = &kinesis{
		stream:            stream,
		shard:             shard,
		shardIteratorType: shardIteratorType,
		client:            gokinesis.New(gokinesis.NewAuth(accessKey, secretKey), region),
	}

	err := k.initShardIterator()
	if err != nil {
		return k, err
	}

	return k, nil
}

func (k *kinesis) args() *gokinesis.RequestArgs {
	args := gokinesis.NewArgs()
	args.Add("StreamName", k.stream)
	args.Add("ShardId", k.shard)
	args.Add("ShardIteratorType", k.shardIteratorType)
	args.Add("ShardIterator", k.shardIterator)
	return args
}

func (k *kinesis) initShardIterator() error {
	resp, err := k.client.GetShardIterator(k.args())
	if err != nil {
		return err
	}

	k.setShardIterator(resp.ShardIterator)
	return nil
}

func (k *kinesis) setShardIterator(shardIter string) {
	if shardIter == "" || len(shardIter) == 0 {
		return
	}

	k.shardIterator = shardIter
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
	client := gokinesis.NewWithEndpoint(gokinesis.NewAuth("BAD_ACCESS_KEY", "BAD_SECRET_KEY"), conf.AWS.Region, endpoint)
	client.CreateStream(stream, 1)

	// Wait for stream to create
	<-time.After(1 * time.Second)

	return client
}
