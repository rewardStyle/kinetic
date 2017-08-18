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

func TestConsumerStatsCollector(t *testing.T) {
	Convey("given a NilConsumerStatsCollector", t, func() {
		var sc ConsumerStatsCollector = &NilConsumerStatsCollector{}
		So(sc, ShouldNotBeNil)

		Convey("check that AddConsumed does not error", func() {
			sc.AddConsumed(1)
		})

		Convey("check that AddDelivered does not error", func() {
			sc.AddDelivered(1)
		})

		Convey("check that AddProcessed does not error", func() {
			sc.AddProcessed(1)
		})

		Convey("check that UpdateBatchSize does not error", func() {
			sc.UpdateBatchSize(1)
		})

		Convey("check that AddGetRecordsCalled does not error", func() {
			sc.AddGetRecordsCalled(1)
		})

		Convey("check that AddReadProvisionedThroughputExceeded does not error", func() {
			sc.AddReadProvisionedThroughputExceeded(1)
		})

		Convey("check that AddGetRecordsTimeout does not error", func() {
			sc.AddGetRecordsTimeout(1)
		})

		Convey("check that AddGetRecordsReadTimeout does not error", func() {
			sc.AddGetRecordsReadTimeout(1)
		})

		Convey("check that UpdateProcessedDuration does not error", func() {
			sc.UpdateProcessedDuration(1)
		})

		Convey("check that UpdateGetRecordsDuration does not error", func() {
			sc.UpdateGetRecordsDuration(1)
		})

		Convey("check that UpdateGetRecordsReadResponseDuration does not error", func() {
			sc.UpdateGetRecordsReadResponseDuration(1)
		})

		Convey("check that UpdateGetRecordsUnmarshalDuration does not error", func() {
			sc.UpdateGetRecordsUnmarshalDuration(1)
		})

		Convey("check that AddCheckpointInsert does not error", func() {
			sc.AddCheckpointInsert(1)
		})

		Convey("check that AddCheckpointDone does not error", func() {
			sc.AddCheckpointDone(1)
		})

		Convey("check that UpdateCheckpointSize does not error", func() {
			sc.UpdateCheckpointSize(1)
		})

		Convey("check that AddCheckpointSent does not error", func() {
			sc.AddCheckpointSent(1)
		})

		Convey("check that AddCheckpointSuccess does not error", func() {
			sc.AddCheckpointSuccess(1)
		})

		Convey("check that AddCheckpointError does not error", func() {
			sc.AddCheckpointError(1)
		})
	})

	Convey("given a DefaultConsumerStatsCollector", t, func() {
		r := metrics.NewRegistry()
		var sc ConsumerStatsCollector = NewDefaultConsumerStatsCollector(r)
		So(sc, ShouldNotBeNil)

		Convey("check that AddConsumed does not error", func() {
			count := rand.Int()
			sc.AddConsumed(count)
			So(sc.(*DefaultConsumerStatsCollector).Consumed.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddDelivered does not error", func() {
			count := rand.Int()
			sc.AddDelivered(count)
			So(sc.(*DefaultConsumerStatsCollector).Delivered.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddProcessed does not error", func() {
			count := rand.Int()
			sc.AddProcessed(count)
			So(sc.(*DefaultConsumerStatsCollector).Processed.Count(), ShouldEqual, int64(count))
		})

		Convey("check that UpdateBatchSize does not error", func() {
			count := rand.Int()
			sc.UpdateBatchSize(count)
			So(sc.(*DefaultConsumerStatsCollector).BatchSize.Value(), ShouldEqual, int64(count))
		})

		Convey("check that AddGetRecordsCalled does not error", func() {
			count := rand.Int()
			sc.AddGetRecordsCalled(count)
			So(sc.(*DefaultConsumerStatsCollector).GetRecordsCalled.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddReadProvisionedThroughputExceeded does not error", func() {
			count := rand.Int()
			sc.AddReadProvisionedThroughputExceeded(count)
			So(sc.(*DefaultConsumerStatsCollector).ReadProvisionedThroughputExceeded.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddGetRecordsTimeout does not error", func() {
			count := rand.Int()
			sc.AddGetRecordsTimeout(count)
			So(sc.(*DefaultConsumerStatsCollector).GetRecordsTimeout.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddGetRecordsReadTimeout does not error", func() {
			count := rand.Int()
			sc.AddGetRecordsReadTimeout(count)
			So(sc.(*DefaultConsumerStatsCollector).GetRecordsReadTimeout.Count(), ShouldEqual, int64(count))
		})

		Convey("check that UpdateProcessedDuration does not error", func() {
			sc.UpdateProcessedDuration(time.Second)
			So(sc.(*DefaultConsumerStatsCollector).ProcessedDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdateGetRecordsDuration does not error", func() {
			sc.UpdateGetRecordsDuration(time.Second)
			So(sc.(*DefaultConsumerStatsCollector).GetRecordsDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdateGetRecordsReadResponseDuration does not error", func() {
			sc.UpdateGetRecordsReadResponseDuration(time.Second)
			So(sc.(*DefaultConsumerStatsCollector).GetRecordsReadResponseDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that UpdateGetRecordsUnmarshalDuration does not error", func() {
			sc.UpdateGetRecordsUnmarshalDuration(time.Second)
			So(sc.(*DefaultConsumerStatsCollector).GetRecordsUnmarshalDuration.Value(), ShouldEqual, 1000000000)
		})

		Convey("check that AddCheckpointInsert does not error", func() {
			count := rand.Int()
			sc.AddCheckpointInsert(count)
			So(sc.(*DefaultConsumerStatsCollector).CheckpointInsert.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddCheckpointDone does not error", func() {
			count := rand.Int()
			sc.AddCheckpointDone(count)
			So(sc.(*DefaultConsumerStatsCollector).CheckpointDone.Count(), ShouldEqual, int64(count))
		})

		Convey("check that UpdateCheckpointSize does not error", func() {
			count := rand.Int()
			sc.UpdateCheckpointSize(count)
			So(sc.(*DefaultConsumerStatsCollector).CheckpointSize.Value(), ShouldEqual, int64(count))
		})

		Convey("check that AddCheckpointSent does not error", func() {
			count := rand.Int()
			sc.AddCheckpointSent(count)
			So(sc.(*DefaultConsumerStatsCollector).CheckpointSent.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddCheckpointSuccess does not error", func() {
			count := rand.Int()
			sc.AddCheckpointSuccess(count)
			So(sc.(*DefaultConsumerStatsCollector).CheckpointSuccess.Count(), ShouldEqual, int64(count))
		})

		Convey("check that AddCheckpointError does not error", func() {
			count := rand.Int()
			sc.AddCheckpointError(count)
			So(sc.(*DefaultConsumerStatsCollector).CheckpointError.Count(), ShouldEqual, int64(count))
		})
	})
}
