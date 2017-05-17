package kinetic

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// TestKineticCreation tests to make sure that the Kinetic creation doesn't return early or return a nil.
func TestKineticCreation(t *testing.T) {
	kinesisKinetic, err := new(kinesis).init("fake", "ShardId-00000001", "TRIM_HORIZON", "BADaccessKey", "BADsecretKey", "region")

	Convey("Given an badly configured init-ed kinetic", t, func() {
		Convey("the error returned should not be nil", func() {
			So(err, ShouldNotBeNil)
		})
		Convey("the returned kinesis struct pointer should not be nil", func() {
			So(kinesisKinetic, ShouldNotBeNil)
		})
		Convey("it should also have some data in it", func() {
			So(kinesisKinetic.stream, ShouldEqual, "fake")
			So(kinesisKinetic.shard, ShouldEqual, "ShardId-00000001")
		})
	})
}
