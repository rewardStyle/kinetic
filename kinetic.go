package kinetic

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	//"github.com/aws/aws-sdk-go/service/firehose"
	"net/http"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// kineticOptions is a struct that holds all of Kinetic's configurable parameters.
type kineticOptions struct {
	awsConfig *aws.Config      //
	logLevel  aws.LogLevelType // log level for configuring the LogHelper's log level
}

// defaultKineticOptions instantiates a kineticOptions with default values.
func defaultKineticOptions() *kineticOptions {
	return &kineticOptions{
		awsConfig: aws.NewConfig().WithHTTPClient(
			&http.Client{
				Timeout: 2 * time.Minute,
			}),
		logLevel: aws.LogOff,
	}
}

// AwsConfigOptionsFn is a method signature for defining functional option methods for configuring Kinetic.
type AwsConfigOptionsFn func(*kineticOptions) error

// AwsConfigCredentials is a functional option method for configuring Kinetic's AwsConfig credentials.
func AwsConfigCredentials(accessKey, secretKey, sessionToken string) AwsConfigOptionsFn {
	return func(o *kineticOptions) error {
		o.awsConfig.WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, sessionToken))
		return nil
	}
}

// AwsConfigRegion is a functional option method for configuring Kinetic's AwsConfig region.
func AwsConfigRegion(region string) AwsConfigOptionsFn {
	return func(o *kineticOptions) error {
		o.awsConfig.WithRegion(region)
		return nil
	}
}

// AwsConfigEndpoint is a functional option method for configuring Kinetic's AwsConfig endpoint.
func AwsConfigEndpoint(endpoint string) AwsConfigOptionsFn {
	return func(o *kineticOptions) error {
		o.awsConfig.WithEndpoint(endpoint)
		return nil
	}
}

// AwsConfigLogger is a functional option method for configuring Kinetic's AwsConfig logger.
func AwsConfigLogger(logger aws.Logger) AwsConfigOptionsFn {
	return func(o *kineticOptions) error {
		o.awsConfig.WithLogger(logger)
		return nil
	}
}

// AwsConfigLogLevel is a functional option method for configuring Kinetic's AwsConfig log level.
func AwsConfigLogLevel(logLevel aws.LogLevelType) AwsConfigOptionsFn {
	return func(o *kineticOptions) error {
		o.awsConfig.WithLogLevel(logLevel)
		return nil
	}
}

// AwsConfigHTTPClientTimeout is a functional option method for configuring Kinetic's
// AwsConfig HTTP client timeout.
func AwsConfigHTTPClientTimeout(timeout time.Duration) AwsConfigOptionsFn {
	return func(o *kineticOptions) error {
		o.awsConfig.WithHTTPClient(&http.Client{
			Timeout: timeout,
		})
		return nil
	}
}

// LogLevel is a functional option method for configuring Kinetic's log level.
func LogLevel(logLevel aws.LogLevelType) AwsConfigOptionsFn {
	return func(o *kineticOptions) error {
		o.logLevel = logLevel & 0xffff0000
		return nil
	}
}

// Kinetic provides access to a Kinesis client and provides some utility methods for interacting
// with the AWS Kinesis service.
type Kinetic struct {
	*kineticOptions
	*LogHelper
	clientMu sync.Mutex
	kclient  kinesisiface.KinesisAPI
	Session  *session.Session
}

// NewKinetic creates a new instance of Kinetic.
func NewKinetic(optionFns ...AwsConfigOptionsFn) (*Kinetic, error) {
	kineticOptions := defaultKineticOptions()
	for _, optionFn := range optionFns {
		optionFn(kineticOptions)
	}
	sess, err := session.NewSession(kineticOptions.awsConfig)
	if err != nil {
		return nil, err
	}
	return &Kinetic{
		kineticOptions: kineticOptions,
		LogHelper: &LogHelper{
			LogLevel: kineticOptions.logLevel,
			Logger:   sess.Config.Logger,
		},
		Session: sess,
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
		return nil, ErrNilDescribeStreamResponse
	}
	if resp.StreamDescription == nil {
		return nil, ErrNilStreamDescription
	}
	var shards []string
	for _, shard := range resp.StreamDescription.Shards {
		if shard.ShardId != nil {
			shards = append(shards, aws.StringValue(shard.ShardId))
		}
	}
	return shards, nil
}
