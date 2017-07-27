package producer

import (
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic"
)

type DebugStatsCollector struct{}

func (l *DebugStatsCollector) AddSentTotal(int)                               {}
func (l *DebugStatsCollector) AddSentSuccess(int)                             {}
func (l *DebugStatsCollector) AddSentFailed(int)                              {}
func (l *DebugStatsCollector) AddSentRetried(int)                             {}
func (l *DebugStatsCollector) AddDroppedTotal(int)                            {}
func (l *DebugStatsCollector) AddDroppedCapacity(int)                         {}
func (l *DebugStatsCollector) AddDroppedRetries(int)                          {}
func (l *DebugStatsCollector) AddPutRecordsProvisionedThroughputExceeded(int) {}
func (l *DebugStatsCollector) AddPutRecordsCalled(int)                        {}
func (l *DebugStatsCollector) AddProvisionedThroughputExceeded(int)           {}
func (l *DebugStatsCollector) AddPutRecordsTimeout(int)                       {}
func (l *DebugStatsCollector) UpdatePutRecordsDuration(time.Duration)         {}
func (l *DebugStatsCollector) UpdatePutRecordsBuildDuration(time.Duration)    {}
func (l *DebugStatsCollector) UpdatePutRecordsSendDuration(time.Duration)     {}
func (l *DebugStatsCollector) UpdateProducerConcurrency(int)                  {}

type DebugStreamWriter struct{}

func (w *DebugStreamWriter) PutRecords(batch []*kinetic.Message) ([]*kinetic.Message, error) {
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
			So(cfg.queueDepth, ShouldEqual, 10000)
			So(cfg.maxRetryAttempts, ShouldEqual, 10)
			So(cfg.concurrency, ShouldEqual, 3)
			So(cfg.shardCheckFreq, ShouldEqual, time.Minute)
			So(cfg.Stats, ShouldHaveSameTypeAs, &NilStatsCollector{})
			So(cfg.LogLevel.Value(), ShouldEqual, kinetic.LogOff)
		})

		Convey("check that we can set both the sdk and kinetic log level", func() {
			ll := aws.LogDebug | aws.LogDebugWithSigning | kinetic.LogDebug
			cfg.SetLogLevel(ll)
			So(cfg.LogLevel.AtLeast(kinetic.LogDebug), ShouldBeTrue)
		})

		Convey("check that we can set the batch size", func() {
			cfg.SetBatchSize(100)
			So(cfg.batchSize, ShouldEqual, 100)
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

		Convey("check that we can set the workers per shard", func() {
			cfg.SetConcurrency(10)
			So(cfg.concurrency, ShouldEqual, 10)
		})

		Convey("check that we can set the data spill callback function", func() {
			fn := func(msg *kinetic.Message) error {
				return nil
			}
			cfg.SetDataSpillFn(fn)
			So(cfg.dataSpillFn, ShouldEqual, fn)
		})

		Convey("check that we can set the check shard frequency", func() {
			cfg.SetShardCheckFreq(time.Second)
			So(cfg.shardCheckFreq, ShouldEqual, time.Second)
		})

		Convey("check that we can configure a stats collector", func() {
			cfg.SetStatsCollector(&DebugStatsCollector{})
			So(cfg.Stats, ShouldHaveSameTypeAs, &DebugStatsCollector{})
		})
	})
}
