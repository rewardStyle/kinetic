package kinetic

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	//"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/rewardStyle/kinetic/errs"
	"github.com/rewardStyle/kinetic/logging"
)

type kineticOptions struct {
}

// Kinetic represents a kinesis and firehose client and provides some utility
// methods for interacting with the AWS services.
type Kinetic struct {
	*kineticOptions
	*logging.LogHelper

	clientMu sync.Mutex
	fclient  firehoseiface.FirehoseAPI
	kclient  kinesisiface.KinesisAPI
	Session  *session.Session
}

// New creates a new instance of Kientic.
func New(fn func(*Config)) (*Kinetic, error) {
	config := NewConfig()
	fn(config)
	session, err := config.GetSession()
	if err != nil {
		return nil, err
	}
	return &Kinetic{
		kineticOptions: config.kineticOptions,
		LogHelper: &logging.LogHelper{
			LogLevel: config.LogLevel,
			Logger:   session.Config.Logger,
		},
		Session: session,
	}, nil
}

func (k *Kinetic) ensureKinesisClient() {
	k.clientMu.Lock()
	defer k.clientMu.Unlock()
	if k.kclient == nil {
		k.kclient = kinesis.New(k.Session)
	}
}

// CreateStream creates a new Kinesis stream.
func (k *Kinetic) CreateStream(stream string, shards int) error {
	k.ensureKinesisClient()
	_, err := k.kclient.CreateStream(&kinesis.CreateStreamInput{
		StreamName: aws.String(stream),
		ShardCount: aws.Int64(int64(shards)),
	})
	if err != nil {
		k.LogError("Error creating kinesis stream:", err)
	}
	return err
}

// WaitUntilStreamExists is meant to be used after CreateStream to wait until a
// Kinesis stream is ACTIVE.
func (k *Kinetic) WaitUntilStreamExists(ctx context.Context, stream string, opts ...request.WaiterOption) error {
	k.ensureKinesisClient()
	return k.kclient.WaitUntilStreamExistsWithContext(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(stream), // Required
	}, opts...)
}

// DeleteStream deletes an existing Kinesis stream.
func (k *Kinetic) DeleteStream(stream string) error {
	k.ensureKinesisClient()
	_, err := k.kclient.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		k.LogError("Error deleting kinesis stream:", err)
	}
	return err
}

// WaitUntilStreamDeleted is meant to be used after DeleteStream to wait until a
// Kinesis stream no longer exists.
func (k *Kinetic) WaitUntilStreamDeleted(ctx context.Context, stream string, opts ...request.WaiterOption) error {
	k.ensureKinesisClient()
	w := request.Waiter{
		Name:        "WaitUntilStreamIsDeleted",
		MaxAttempts: 18,
		Delay:       request.ConstantWaiterDelay(10 * time.Second),
		Acceptors: []request.WaiterAcceptor{
			{
				State:    request.SuccessWaiterState,
				Matcher:  request.ErrorWaiterMatch,
				Expected: kinesis.ErrCodeResourceNotFoundException,
			},
		},
		Logger: k.Session.Config.Logger,
		NewRequest: func(opts []request.Option) (*request.Request, error) {
			req, _ := k.kclient.DescribeStreamRequest(&kinesis.DescribeStreamInput{
				StreamName: aws.String(stream), // Required
			})
			req.SetContext(ctx)
			req.ApplyOptions(opts...)
			return req, nil
		},
	}
	w.ApplyOptions(opts...)
	return w.WaitWithContext(ctx)
}

// GetShards returns a list of the shards in a Kinesis stream.
func (k *Kinetic) GetShards(stream string) ([]string, error) {
	k.ensureKinesisClient()
	resp, err := k.kclient.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		k.LogError("Error describing kinesis stream:", err)
		return nil, err
	}
	if resp == nil {
		return nil, errs.ErrNilDescribeStreamResponse
	}
	if resp.StreamDescription == nil {
		return nil, errs.ErrNilStreamDescription
	}
	var shards []string
	for _, shard := range resp.StreamDescription.Shards {
		if shard.ShardId != nil {
			shards = append(shards, aws.StringValue(shard.ShardId))
		}
	}
	return shards, nil
}
