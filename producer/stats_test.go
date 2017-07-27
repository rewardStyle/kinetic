package producer

import (
	"math/rand"
	"testing"
	"time"

	metrics "github.com/jasonyurs/go-metrics"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestStatsCollector(t *testing.T) {
	Convey("given a NilStatsCollector", t, func() {
		var sc StatsCollector = &NilStatsCollector{}
		So(sc, ShouldNotBeNil)

		Convey("check that AddSentTotal does not error", func() {
			sc.AddSentTotal(1)
		})

		Convey("check that AddSentSuccess does not error", func() {
			sc.AddSentSuccess(1)
		})

		Convey("check that AddSentFailed does not error", func() {
			sc.AddSentFailed(1)
		})

		Convey("check that AddSentRetried does not error", func() {
			sc.AddSentRetried(1)
		})

		Convey("check that AddDroppedTotal does not error", func() {
			sc.AddDroppedTotal(1)
		})

		Convey("check that AddDroppedCapacity does not error", func() {
			sc.AddDroppedCapacity(1)
		})

		Convey("check that AddDroppedRetries does not error", func() {
			sc.AddDroppedRetries(1)
		})

		Convey("check that AddPutRecordsProvisionedThroughputExceeded does not error", func() {
			sc.AddPutRecordsProvisionedThroughputExceeded(1)
		})

		Convey("check that AddPutRecordsCalled does not eroror", func() {
			sc.AddPutRecordsCalled(1)
		})

		Convey("check that AddProvisionedThroughputExceeded does not erro", func() {
			sc.AddProvisionedThroughputExceeded(1)
		})

		Convey("check that AddPutRecordsTimeout does not error", func() {
			sc.AddPutRecordsTimeout(1)
		})

		Convey("check that UpdatePutRecordsDuration does not error", func() {
			sc.UpdatePutRecordsDuration(time.Second)
		})

		Convey("check that UpdatePutRecordsBuildDuration does not error", func() {
			sc.UpdatePutRecordsBuildDuration(time.Second)
		})

		Convey("check that UpdatePutRecordsSendDuration does not error", func() {
			sc.UpdatePutRecordsSendDuration(time.Second)
		})

		Convey("check that UpdateProducerConcurrency does not error", func() {
			sc.UpdateProducerConcurrency(5)
		})
	})

	Convey("given a DefaulStatsCollector", t, func() {
		r := metrics.NewRegistry()
		var sc StatsCollector = NewDefaultStatsCollector(r)
		So(sc, ShouldNotBeNil)

		Convey("check that AddSentTotal does not error", func() {
			count := rand.Int()
			sc.AddSentTotal(count)
			So(sc.(*DefaultStatsCollector).SentTotal.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddSentSuccess does not error", func() {
			count := rand.Int()
			sc.AddSentSuccess(count)
			So(sc.(*DefaultStatsCollector).SentSuccess.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddSentFailed does not error", func() {
			count := rand.Int()
			sc.AddSentFailed(count)
			So(sc.(*DefaultStatsCollector).SentFailed.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddSentRetried does not error", func() {
			count := rand.Int()
			sc.AddSentRetried(count)
			So(sc.(*DefaultStatsCollector).SentRetried.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddDroppedTotal does not error", func() {
			count := rand.Int()
			sc.AddDroppedTotal(count)
			So(sc.(*DefaultStatsCollector).DroppedTotal.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddDroppedCapacity does not error", func() {
			count := rand.Int()
			sc.AddDroppedCapacity(count)
			So(sc.(*DefaultStatsCollector).DroppedCapacity.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddDroppedRetries does not error", func() {
			count := rand.Int()
			sc.AddDroppedRetries(count)
			So(sc.(*DefaultStatsCollector).DroppedRetries.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddPutRecordsProvisionedThroughputExceeded does not error", func() {
			count := rand.Int()
			sc.AddPutRecordsProvisionedThroughputExceeded(count)
			So(sc.(*DefaultStatsCollector).PutRecordsProvisionedThroughputExceeded.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddPutRecordsCalled does not eroror", func() {
			count := rand.Int()
			sc.AddPutRecordsCalled(count)
			So(sc.(*DefaultStatsCollector).PutRecordsCalled.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddProvisionedThroughputExceeded does not erro", func() {
			count := rand.Int()
			sc.AddProvisionedThroughputExceeded(count)
			So(sc.(*DefaultStatsCollector).ProvisionedThroughputExceeded.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddPutRecordsTimeout does not error", func() {
			count := rand.Int()
			sc.AddPutRecordsTimeout(count)
			So(sc.(*DefaultStatsCollector).PutRecordsTimeout.Count(), ShouldEqual, int64(count))
		})

		Convey("check that UpdatePutRecordsDuration does not error", func() {
			sc.UpdatePutRecordsDuration(time.Second)
			So(sc.(*DefaultStatsCollector).PutRecordsDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdatePutRecordsBuildDuration does not error", func() {
			sc.UpdatePutRecordsBuildDuration(time.Second)
			So(sc.(*DefaultStatsCollector).PutRecordsBuildDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdatePutRecordsSendDuration does not error", func() {
			sc.UpdatePutRecordsSendDuration(time.Second)
			So(sc.(*DefaultStatsCollector).PutRecordsSendDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdateProducerConcurrency does not error", func() {
			sc.UpdateProducerConcurrency(5)
			So(sc.(*DefaultStatsCollector).ProducerConcurrency.Value(), ShouldEqual, 5)
		})
	})
}
