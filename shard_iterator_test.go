package kinetic

import (
	"github.com/aws/aws-sdk-go/aws"
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"
)

func TestShardIterator(t *testing.T) {
	Convey("given a new shard iterator", t, func() {
		it := NewShardIterator()

		Convey("check that the default shard iterator type is TRIM_HORIZON", func() {
			So(it.shardIteratorType, ShouldEqual, "TRIM_HORIZON")
			So(it.getStartingSequenceNumber(), ShouldBeNil)
			So(it.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can explicitly set it to TRIM_HORIZON", func() {
			it = it.TrimHorizon()
			So(it.shardIteratorType, ShouldEqual, "TRIM_HORIZON")
			So(it.getStartingSequenceNumber(), ShouldBeNil)
			So(it.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can explicitly set it to LATEST", func() {
			it = it.Latest()
			So(it.shardIteratorType, ShouldEqual, "LATEST")
			So(it.getStartingSequenceNumber(), ShouldBeNil)
			So(it.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can explicitly set it to AT_SEQEUENCE_NUMBER", func() {
			it = it.AtSequenceNumber("some-sequence")
			So(it.shardIteratorType, ShouldEqual, "AT_SEQUENCE_NUMBER")
			So(aws.StringValue(it.getStartingSequenceNumber()), ShouldEqual, "some-sequence")
			So(it.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can explicitly set it to AFTER_SEQEUENCE_NUMBER", func() {
			it = it.AfterSequenceNumber("some-sequence")
			So(it.shardIteratorType, ShouldEqual, "AFTER_SEQUENCE_NUMBER")
			So(aws.StringValue(it.getStartingSequenceNumber()), ShouldEqual, "some-sequence")
			So(it.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can explicitly set it to AT_TIMESTAMP", func() {
			n := time.Now()
			it = it.AtTimestamp(n)
			So(it.shardIteratorType, ShouldEqual, "AT_TIMESTAMP")
			So(aws.TimeValue(it.getTimestamp()).Equal(n), ShouldBeTrue)
			So(it.getStartingSequenceNumber(), ShouldBeNil)
		})
	})
}
