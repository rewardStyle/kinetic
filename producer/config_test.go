package producer

import (
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/logging"
	"github.com/rewardStyle/kinetic/message"
)

type DebugStatsCollector struct{}

func (l *DebugStatsCollector) AddSent(int)                                    {}
func (l *DebugStatsCollector) AddFailed(int)                                  {}
func (l *DebugStatsCollector) AddDropped(int)                                 {}
func (l *DebugStatsCollector) AddBatchSize(int)                               {}
func (l *DebugStatsCollector) AddPutRecordsProvisionedThroughputExceeded(int) {}
func (l *DebugStatsCollector) AddPutRecordsCalled(int)                        {}
func (l *DebugStatsCollector) AddProvisionedThroughputExceeded(int)           {}
func (l *DebugStatsCollector) AddPutRecordsTimeout(int)                       {}
func (l *DebugStatsCollector) AddPutRecordsDuration(time.Duration)            {}
func (l *DebugStatsCollector) AddPutRecordsBuildDuration(time.Duration)       {}
func (l *DebugStatsCollector) AddPutRecordsSendDuration(time.Duration)        {}

func getSession(config *Config) *session.Session {
	sess, err := config.GetSession()
	So(err, ShouldBeNil)
	So(sess, ShouldNotBeNil)
	return sess
}

type DebugStreamWriter struct{}

func (w *DebugStreamWriter) PutRecords(batch []*message.Message) ([]*message.Message, error) {
	return nil, nil
}

func (w *DebugStreamWriter) AssociateProducer(producer Producer) error {
	return nil
}

func TestNewConfig(t *testing.T) {
	Convey("given a new producer config", t, func() {
		config := NewConfig()

		Convey("check the default values for its non-zero config", func() {
			So(config.batchSize, ShouldEqual, 500)
			So(config.batchTimeout, ShouldEqual, 1*time.Second)
			So(config.queueDepth, ShouldEqual, 500)
			So(config.maxRetryAttempts, ShouldEqual, 10)
			So(config.concurrency, ShouldEqual, 1)
			So(config.Stats, ShouldHaveSameTypeAs, &NilStatsCollector{})
			So(config.LogLevel.Value(), ShouldEqual, logging.LogOff)
		})

		Convey("check that we can retrieve an aws.Session from it ", func() {
			getSession(config)
		})

		Convey("check that we can set both the sdk and kinetic log level", func() {
			ll := aws.LogDebug | aws.LogDebugWithSigning | logging.LogDebug
			config.SetLogLevel(ll)
			sess := getSession(config)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeTrue)
			So(sess.Config.LogLevel.Matches(aws.LogDebugWithSigning), ShouldBeTrue)
			So(config.LogLevel.AtLeast(logging.LogDebug), ShouldBeTrue)
		})

		Convey("check that we can set the AWS configuration", func() {
			k, err := kinetic.New(func(c *kinetic.Config) {
				c.SetEndpoint("bogus-endpoint")
			})
			So(err, ShouldBeNil)
			config.SetAwsConfig(k.Session.Config)
			sess := getSession(config)
			So(aws.StringValue(sess.Config.Endpoint), ShouldEqual, "bogus-endpoint")
		})

		Convey("check that we can set the batch size", func() {
			config.SetBatchSize(1000)
			So(config.batchSize, ShouldEqual, 1000)
		})

		Convey("check that we can set the batch timeout", func() {
			config.SetBatchTimeout(10 * time.Second)
			So(config.batchTimeout, ShouldEqual, 10*time.Second)
		})

		Convey("check that we can set the queue depth", func() {
			config.SetQueueDepth(1000)
			So(config.queueDepth, ShouldEqual, 1000)
		})

		Convey("check that we can set the max retries", func() {
			config.SetMaxRetryAttempts(100)
			So(config.maxRetryAttempts, ShouldEqual, 100)
		})

		Convey("check that we can set the concurrency limit", func() {
			config.SetConcurrency(50)
			So(config.concurrency, ShouldEqual, 50)
		})

		Convey("check that we can configure a stats collector", func() {
			config.SetStatsCollector(&DebugStatsCollector{})
			So(config.Stats, ShouldHaveSameTypeAs, &DebugStatsCollector{})
		})

		Convey("check that we can configure a kinesis stream", func() {
			stream := "some-stream"
			config.SetKinesisStream(stream)
			So(config.writer, ShouldHaveSameTypeAs, &KinesisWriter{})
			So(config.writer.(*KinesisWriter).stream, ShouldEqual, stream)
		})
	})
}
