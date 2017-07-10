package producer

import (
	. "github.com/smartystreets/goconvey/convey"
	"math/rand"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestStatsCollector(t *testing.T) {
	Convey("given a NilStatsCollector", t, func() {
		var sc StatsCollector = &NilStatsCollector{}
		So(sc, ShouldNotBeNil)

		Convey("check that AddSent does not error", func() {
			sc.AddSent(1)
		})

		Convey("check that AddFailed does not error", func() {
			sc.AddFailed(1)
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

		Convey("check that AddBatchSize does not error", func() {
			sc.AddBatchSize(1)
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

		Convey("check that AddPutRecordsDuration does not error", func() {
			sc.AddPutRecordsDuration(time.Second)
		})

		Convey("check that AddPutRecordsBuildDuration does not error", func() {
			sc.AddPutRecordsBuildDuration(time.Second)
		})

		Convey("check that AddPutRecordsSendDuration does not error", func() {
			sc.AddPutRecordsSendDuration(time.Second)
		})
	})

	Convey("given a DefaulStatsCollector", t, func() {
		r := metrics.NewRegistry()
		var sc StatsCollector = NewDefaultStatsCollector(r)
		So(sc, ShouldNotBeNil)

		Convey("check that AddSent does not error", func() {
			count := rand.Int()
			sc.AddSent(count)
			So(sc.(*DefaultStatsCollector).Sent.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddFailed does not error", func() {
			count := rand.Int()
			sc.AddFailed(count)
			So(sc.(*DefaultStatsCollector).Failed.Count(), ShouldEqual, int64(count))
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

		Convey("check that AddBatchSize does not error", func() {
			count := rand.Int()
			sc.AddBatchSize(count)
			So(sc.(*DefaultStatsCollector).BatchSize.Count(), ShouldEqual, int64(count))
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

		Convey("check that AddPutRecordsDuration does not error", func() {
			sc.AddPutRecordsDuration(time.Second)
			So(sc.(*DefaultStatsCollector).PutRecordsDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that AddPutRecordsBuildDuration does not error", func() {
			sc.AddPutRecordsBuildDuration(time.Second)
			So(sc.(*DefaultStatsCollector).PutRecordsBuildDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that AddPutRecordsSendDuration does not error", func() {
			sc.AddPutRecordsSendDuration(time.Second)
			So(sc.(*DefaultStatsCollector).PutRecordsSendDuration.Value(), ShouldEqual, 1000000000)
		})
	})
}
