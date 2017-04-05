package kinetic

import (
	. "github.com/smartystreets/goconvey/convey"

	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func TestKinetic(t *testing.T) {
	Convey("given a kinetic object", t, func() {
		k, err := New(func(c *Config) {
			c.SetCredentials("some-access-key", "some-secret-key", "some-security-token")
			c.SetRegion("some-region")
			c.SetEndpoint("http://127.0.0.1:4567")
		})
		So(k, ShouldNotBeNil)
		So(err, ShouldBeNil)

		stream := "some-kinetic-stream"

		Convey("check that calling ensureKinesisClient twice doesn't overwrite existing client", func() {
			So(k.kclient, ShouldBeNil)
			k.ensureKinesisClient()
			So(k.kclient, ShouldNotBeNil)
			kclient := k.kclient
			k.ensureKinesisClient()
			So(k.kclient, ShouldEqual, kclient)
		})

		Convey("check deleting a non-existent stream returns an error", func() {
			err := k.DeleteStream(stream)
			So(err, ShouldNotBeNil)
			e := err.(awserr.Error)
			So(e.Code(), ShouldEqual, kinesis.ErrCodeResourceNotFoundException)
		})

		Convey("check getting shards on a non-existent stream returns an error", func() {
			shards, err := k.GetShards(stream)
			So(shards, ShouldBeNil)
			So(err, ShouldNotBeNil)
			e := err.(awserr.Error)
			So(e.Code(), ShouldEqual, kinesis.ErrCodeResourceNotFoundException)
		})

		Convey("check that we can create a stream", func() {
			err := k.CreateStream(stream, 1)
			So(err, ShouldBeNil)
			err = k.WaitUntilStreamExists(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
			So(err, ShouldBeNil)

			Convey("check that creating an existing stream returns an error", func() {
				err := k.CreateStream(stream, 1)
				So(err, ShouldNotBeNil)
				e := err.(awserr.Error)
				So(e.Code(), ShouldEqual, kinesis.ErrCodeResourceInUseException)
			})

			Convey("check that we can delete an existing stream", func() {
				err := k.DeleteStream(stream)
				So(err, ShouldBeNil)
				err = k.WaitUntilStreamDeleted(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
				So(err, ShouldBeNil)
			})

			Convey("check that we can obtain a list of shards", func() {
				shards, err := k.GetShards(stream)
				So(err, ShouldBeNil)
				So(len(shards), ShouldEqual, 1)
			})

			Reset(func() {
				k.DeleteStream(stream)
				k.WaitUntilStreamDeleted(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
			})
		})
	})
}
