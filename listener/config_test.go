package listener

import (
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/logging"
)

type DebugStatsListener struct{}

func (l *DebugStatsListener) AddConsumedSample(int)                       {}
func (l *DebugStatsListener) AddDeliveredSample(int)                      {}
func (l *DebugStatsListener) AddProcessedSample(int)                      {}
func (l *DebugStatsListener) AddBatchSizeSample(int)                      {}
func (l *DebugStatsListener) AddGetRecordsCalled(int)                     {}
func (l *DebugStatsListener) AddProvisionedThroughputExceeded(int)        {}
func (l *DebugStatsListener) AddGetRecordsTimeout(int)                    {}
func (l *DebugStatsListener) AddGetRecordsReadTimeout(int)                {}
func (l *DebugStatsListener) AddProcessedTime(time.Duration)              {}
func (l *DebugStatsListener) AddGetRecordsTime(time.Duration)             {}
func (l *DebugStatsListener) AddGetRecordsReadResponseTime(time.Duration) {}
func (l *DebugStatsListener) AddGetRecordsUnmarshalTime(time.Duration)    {}

func getSession(config *Config) *session.Session {
	sess, err := config.GetSession()
	So(err, ShouldBeNil)
	So(sess, ShouldNotBeNil)
	return sess
}

func TestNewConfig(t *testing.T) {
	Convey("given a new listener config", t, func() {
		stream := "some-stream"
		shard := "some-shard"
		config := NewConfig(stream, shard)

		Convey("check the default values for its non-zero config", func() {
			So(config.stream, ShouldEqual, stream)
			So(config.shard, ShouldEqual, shard)
			So(config.batchSize, ShouldEqual, 10000)
			So(config.concurrency, ShouldEqual, 10000)
			So(config.shardIterator.shardIteratorType, ShouldEqual, "TRIM_HORIZON")
			So(config.getRecordsReadTimeout, ShouldEqual, 1*time.Second)
			So(config.stats, ShouldHaveSameTypeAs, &NilStatsListener{})
			So(config.logLevel.Value(), ShouldEqual, logging.LogOff)
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
			So(config.logLevel.AtLeast(logging.LogDebug), ShouldBeTrue)
		})

		Convey("check that we can import configuration from kinetic", func() {
			k, err := kinetic.New(func(c *kinetic.Config) {
				c.SetEndpoint("bogus-endpoint")
			})
			So(err, ShouldBeNil)
			config = config.FromKinetic(k)
			sess := getSession(config)
			So(aws.StringValue(sess.Config.Endpoint), ShouldEqual, "bogus-endpoint")
		})

		Convey("check that we can set the batch size", func() {
			config.SetBatchSize(1000)
			So(config.batchSize, ShouldEqual, 1000)
		})

		Convey("check that we can set the concurrency limit", func() {
			config.SetConcurrency(50)
			So(config.concurrency, ShouldEqual, 50)
		})

		Convey("check that the default shard iterator is TRIM_HORIZON", func() {
			config.SetInitialShardIterator(NewShardIterator())
			So(config.shardIterator.shardIteratorType, ShouldEqual, "TRIM_HORIZON")
			So(config.shardIterator.getStartingSequenceNumber(), ShouldBeNil)
			So(config.shardIterator.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can set the initial shard iterator (to LATEST)", func() {
			config.SetInitialShardIterator(NewShardIterator().Latest())
			So(config.shardIterator.shardIteratorType, ShouldEqual, "LATEST")
			So(config.shardIterator.getStartingSequenceNumber(), ShouldBeNil)
			So(config.shardIterator.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can set the read timeout for the GetRecords request", func() {
			config.SetGetRecordsReadTimeout(10 * time.Second)
			So(config.getRecordsReadTimeout, ShouldEqual, 10*time.Second)
		})

		Convey("check that we can configure a stats listener", func() {
			config.SetStatsListener(&DebugStatsListener{})
			So(config.stats, ShouldHaveSameTypeAs, &DebugStatsListener{})
		})
	})
}
