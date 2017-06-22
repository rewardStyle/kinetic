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

type DebugStatsCollector struct{}

func (l *DebugStatsCollector) AddConsumed(int)                                 {}
func (l *DebugStatsCollector) AddDelivered(int)                                {}
func (l *DebugStatsCollector) AddProcessed(int)                                {}
func (l *DebugStatsCollector) AddBatchSize(int)                                {}
func (l *DebugStatsCollector) AddGetRecordsCalled(int)                         {}
func (l *DebugStatsCollector) AddProvisionedThroughputExceeded(int)            {}
func (l *DebugStatsCollector) AddGetRecordsTimeout(int)                        {}
func (l *DebugStatsCollector) AddGetRecordsReadTimeout(int)                    {}
func (l *DebugStatsCollector) AddProcessedDuration(time.Duration)              {}
func (l *DebugStatsCollector) AddGetRecordsDuration(time.Duration)             {}
func (l *DebugStatsCollector) AddGetRecordsReadResponseDuration(time.Duration) {}
func (l *DebugStatsCollector) AddGetRecordsUnmarshalDuration(time.Duration)    {}

func getSession(config *Config) *session.Session {
	sess, err := config.GetSession()
	So(err, ShouldBeNil)
	So(sess, ShouldNotBeNil)
	return sess
}

func TestNewConfig(t *testing.T) {
	Convey("given a new listener config", t, func() {
		k, err := kinetic.New(func(c *kinetic.Config) {
			c.SetEndpoint("bogus-endpoint")
		})
		So(k, ShouldNotBeNil)
		So(err, ShouldBeNil)
		config := NewConfig(k.Session.Config)

		Convey("check the default values for its non-zero config", func() {
			So(config.queueDepth, ShouldEqual, 10000)
			So(config.concurrency, ShouldEqual, 10000)
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

		Convey("check that we can set the concurrency limit", func() {
			config.SetConcurrency(50)
			So(config.concurrency, ShouldEqual, 50)
		})

		//TODO: Move this test to kinesisreaderconfig_test.go
		//Convey("check that we can set the read timeout for the GetRecords request", func() {
		//	config.SetReadTimeout(10 * time.Second)
		//	So(config.readTimeout, ShouldEqual, 10*time.Second)
		//})

		Convey("check that we can configure a stats collector", func() {
			config.SetStatsCollector(&DebugStatsCollector{})
			So(config.Stats, ShouldHaveSameTypeAs, &DebugStatsCollector{})
		})
	})
}
