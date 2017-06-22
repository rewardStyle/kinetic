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

func TestNewConfig(t *testing.T) {
	Convey("given a new producer config", t, func() {
		k, err := kinetic.New(func(c *kinetic.Config) {
			c.SetEndpoint("bogus-endpoint")
		})
		So(err, ShouldBeNil)
		cfg := NewConfig(k.Session.Config)

		Convey("check the default values for its non-zero config", func() {
			So(cfg.batchSize, ShouldEqual, 500)
			So(cfg.batchTimeout, ShouldEqual, 1*time.Second)
			So(cfg.queueDepth, ShouldEqual, 500)
			So(cfg.maxRetryAttempts, ShouldEqual, 10)
			So(cfg.concurrency, ShouldEqual, 1)
			So(cfg.Stats, ShouldHaveSameTypeAs, &NilStatsCollector{})
			So(cfg.LogLevel.Value(), ShouldEqual, logging.LogOff)
		})

		Convey("check that we can retrieve an aws.Session from it ", func() {
			getSession(cfg)
		})

		Convey("check that we can set both the sdk and kinetic log level", func() {
			ll := aws.LogDebug | aws.LogDebugWithSigning | logging.LogDebug
			cfg.SetLogLevel(ll)
			sess := getSession(cfg)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeTrue)
			So(sess.Config.LogLevel.Matches(aws.LogDebugWithSigning), ShouldBeTrue)
			So(cfg.LogLevel.AtLeast(logging.LogDebug), ShouldBeTrue)
		})

		Convey("check that we can set the batch size", func() {
			cfg.SetBatchSize(1000)
			So(cfg.batchSize, ShouldEqual, 1000)
		})

		Convey("check that we can set the batch timeout", func() {
			cfg.SetBatchTimeout(10 * time.Second)
			So(cfg.batchTimeout, ShouldEqual, 10*time.Second)
		})

		Convey("check that we can set the queue depth", func() {
			cfg.SetQueueDepth(1000)
			So(cfg.queueDepth, ShouldEqual, 1000)
		})

		Convey("check that we can set the max retries", func() {
			cfg.SetMaxRetryAttempts(100)
			So(cfg.maxRetryAttempts, ShouldEqual, 100)
		})

		Convey("check that we can set the concurrency limit", func() {
			cfg.SetConcurrency(50)
			So(cfg.concurrency, ShouldEqual, 50)
		})

		Convey("check that we can configure a stats collector", func() {
			cfg.SetStatsCollector(&DebugStatsCollector{})
			So(cfg.Stats, ShouldHaveSameTypeAs, &DebugStatsCollector{})
		})
	})
}
