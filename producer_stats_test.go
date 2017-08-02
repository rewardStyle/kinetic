package kinetic

import (
	"math/rand"
	"testing"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	. "github.com/smartystreets/goconvey/convey"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestProducerStatsCollector(t *testing.T) {
	Convey("given a NilProducerStatsCollector", t, func() {
		var sc ProducerStatsCollector = &NilProducerStatsCollector{}
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

		Convey("check that WriteProvisionedThroughputExceeded does not error", func() {
			sc.AddWriteProvisionedThroughputExceeded(1)
		})

		Convey("check that AddPutRecordsCalled does not eroror", func() {
			sc.AddPutRecordsCalled(1)
		})

		Convey("check that AddProvisionedThroughputExceeded does not erro", func() {
			sc.AddWriteProvisionedThroughputExceeded(1)
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

	Convey("given a DefaultProdcuerStatsCollector", t, func() {
		r := metrics.NewRegistry()
		var sc ProducerStatsCollector = NewDefaultProducerStatsCollector(r)
		So(sc, ShouldNotBeNil)

		Convey("check that AddSentTotal does not error", func() {
			count := rand.Int()
			sc.AddSentTotal(count)
			So(sc.(*DefaultProducerStatsCollector).SentTotal.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddSentSuccess does not error", func() {
			count := rand.Int()
			sc.AddSentSuccess(count)
			So(sc.(*DefaultProducerStatsCollector).SentSuccess.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddSentFailed does not error", func() {
			count := rand.Int()
			sc.AddSentFailed(count)
			So(sc.(*DefaultProducerStatsCollector).SentFailed.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddSentRetried does not error", func() {
			count := rand.Int()
			sc.AddSentRetried(count)
			So(sc.(*DefaultProducerStatsCollector).SentRetried.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddDroppedTotal does not error", func() {
			count := rand.Int()
			sc.AddDroppedTotal(count)
			So(sc.(*DefaultProducerStatsCollector).DroppedTotal.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddDroppedCapacity does not error", func() {
			count := rand.Int()
			sc.AddDroppedCapacity(count)
			So(sc.(*DefaultProducerStatsCollector).DroppedCapacity.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddDroppedRetries does not error", func() {
			count := rand.Int()
			sc.AddDroppedRetries(count)
			So(sc.(*DefaultProducerStatsCollector).DroppedRetries.Count(), ShouldEqual, int64(count))
		})

		Convey("check that WriteProvisionedThroughputExceeded does not error", func() {
			count := rand.Int()
			sc.AddWriteProvisionedThroughputExceeded(count)
			So(sc.(*DefaultProducerStatsCollector).PutRecordsProvisionedThroughputExceeded.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddPutRecordsCalled does not eroror", func() {
			count := rand.Int()
			sc.AddPutRecordsCalled(count)
			So(sc.(*DefaultProducerStatsCollector).PutRecordsCalled.Count(), ShouldEqual, int64(count))
		})

		Convey("check that WriteProvisionedThroughputExceeded does not erro", func() {
			count := rand.Int()
			sc.AddWriteProvisionedThroughputExceeded(count)
			So(sc.(*DefaultProducerStatsCollector).PutRecordsProvisionedThroughputExceeded.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddPutRecordsTimeout does not error", func() {
			count := rand.Int()
			sc.AddPutRecordsTimeout(count)
			So(sc.(*DefaultProducerStatsCollector).PutRecordsTimeout.Count(), ShouldEqual, int64(count))
		})

		Convey("check that UpdatePutRecordsDuration does not error", func() {
			sc.UpdatePutRecordsDuration(time.Second)
			So(sc.(*DefaultProducerStatsCollector).PutRecordsDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdatePutRecordsBuildDuration does not error", func() {
			sc.UpdatePutRecordsBuildDuration(time.Second)
			So(sc.(*DefaultProducerStatsCollector).PutRecordsBuildDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdatePutRecordsSendDuration does not error", func() {
			sc.UpdatePutRecordsSendDuration(time.Second)
			So(sc.(*DefaultProducerStatsCollector).PutRecordsSendDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdateProducerConcurrency does not error", func() {
			sc.UpdateProducerConcurrency(5)
			So(sc.(*DefaultProducerStatsCollector).ProducerConcurrency.Value(), ShouldEqual, 5)
		})
	})
}
