package producer

import (
	. "github.com/smartystreets/goconvey/convey"

	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"

	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/listener"
	"github.com/rewardStyle/kinetic/message"
)

func TestProducer(t *testing.T) {
	Convey("given a producer", t, func() {
		k, err := kinetic.New(func(c *kinetic.Config) {
			c.SetCredentials("some-access-key", "some-secret-key", "some-security-token")
			c.SetRegion("some-region")
			c.SetEndpoint("http://127.0.0.1:4567")
		})

		stream := "some-producer-stream"

		err = k.CreateStream(stream, 1)
		So(err, ShouldBeNil)

		err = k.WaitUntilStreamExists(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		So(err, ShouldBeNil)

		shards, err := k.GetShards(stream)
		So(err, ShouldBeNil)
		So(len(shards), ShouldEqual, 1)

		p, err := NewProducer(func(c *Config) {
			c.SetAwsConfig(k.Session.Config)
			c.SetKinesisStream(stream)
			c.SetBatchSize(5)
			c.SetBatchTimeout(1 * time.Second)
			So(err, ShouldBeNil)
		})
		So(p, ShouldNotBeNil)
		So(err, ShouldBeNil)

		l, err := listener.NewListener(func(c *listener.Config) {
			c.SetAwsConfig(k.Session.Config)
			c.SetConcurrency(10)
		})
		So(l, ShouldNotBeNil)
		So(err, ShouldBeNil)

		Convey("given a kinesis writer", func() {
			w := p.writer.(*KinesisWriter)

			Convey("check that the writer was initialized with the correct stream name", func() {
				So(w.stream, ShouldEqual, stream)
			})

			Convey("check that the writer has a valid reference to the producer", func() {
				So(w.producer, ShouldEqual, p)
			})

			Convey("check that calling ensureClient twice doesn't overwrite existing client", func() {
				So(w.client, ShouldBeNil)
				w.ensureClient()
				So(w.client, ShouldNotBeNil)
				client := w.client
				w.ensureClient()
				So(w.client, ShouldEqual, client)
			})
		})

		Convey("check that we can send a single message after batch timeout elapses", func() {
			start := time.Now()
			data := "hello"
			p.Send(&message.Message{
				PartitionKey: aws.String("key"),
				Data:         []byte(data),
			})
			msg, err := l.Retrieve()
			elapsed := time.Since(start)
			Printf("(send took %f seconds)\n", elapsed.Seconds())
			So(err, ShouldBeNil)
			So(string(msg.Data), ShouldEqual, data)
			So(elapsed.Seconds(), ShouldBeGreaterThan, 1)
		})

		Convey("check that we can send a batch of messages after batch size is reached", func() {
			start := time.Now()
			data := []string{"hello1", "hello2", "hello3", "hello4", "hello5", "hello6"}
			for _, datum := range data {
				p.Send(&message.Message{
					PartitionKey: aws.String("key"),
					Data:         []byte(datum),
				})
			}

			for i := 0; i < 5; i++ {
				msg, err := l.Retrieve()
				So(err, ShouldBeNil)
				So(string(msg.Data), ShouldEqual, data[i])
			}
			elapsed := time.Since(start)
			So(elapsed.Seconds(), ShouldBeLessThan, 1)
			Printf("(first 5 took %f seconds)\n", elapsed.Seconds())

			msg, err := l.Retrieve()
			So(err, ShouldBeNil)
			So(string(msg.Data), ShouldEqual, data[5])
			elapsed = time.Since(start)
			So(elapsed.Seconds(), ShouldBeGreaterThan, 1)
			Printf("(last took %f seconds)\n", elapsed.Seconds())
		})

		Reset(func() {
			k.DeleteStream(stream)
			k.WaitUntilStreamDeleted(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		})
	})
}
