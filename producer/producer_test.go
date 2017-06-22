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

		err = k.WaitUntilStreamExists(context.TODO(), stream,
			request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		So(err, ShouldBeNil)

		shards, err := k.GetShards(stream)
		So(err, ShouldBeNil)
		So(len(shards), ShouldEqual, 1)

		So(k.Session, ShouldNotBeNil)
		So(k.Session.Config, ShouldNotBeNil)
		w, err := NewKinesisWriter(k.Session.Config, stream)
		So(w, ShouldNotBeNil)
		So(err, ShouldBeNil)

		p, err := NewProducer(k.Session.Config, w, func(c *Config) {
			c.SetBatchSize(5)
			c.SetBatchTimeout(1000 * time.Millisecond)
			c.SetConcurrency(10)
			c.SetQueueDepth(10)
		})
		So(p, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(k.Session, ShouldNotBeNil)
		So(k.Session.Config, ShouldNotBeNil)
		r, err := listener.NewKinesisReader(k.Session.Config, stream, shards[0],
			func(krc *listener.KinesisReaderConfig) {
				krc.SetReadTimeout(1000 * time.Millisecond)
		})
		So(r, ShouldNotBeNil)
		So(err, ShouldBeNil)

		l, err := listener.NewListener(k.Session.Config, r, func(c *listener.Config) {
			c.SetQueueDepth(10)
			c.SetConcurrency(10)
		})
		So(l, ShouldNotBeNil)
		So(err, ShouldBeNil)

		Convey("given a kinesis writer", func() {
			w := p.writer.(*KinesisWriter)

			Convey("check that the writer was initialized with the correct stream name", func() {
				So(w.stream, ShouldEqual, stream)
			})

			Convey("check that the writer was initialized correctly", func() {
				So(w.client, ShouldNotBeNil)
			})
		})

		Convey("check that we can send and receive a single message", func(){
			start := time.Now()
			data := "hello"
			p.Send(&message.Message{
				PartitionKey: aws.String("key"),
				Data: []byte(data),
			})
			msg, err := l.RetrieveWithContext(context.TODO())
			elapsed := time.Since(start)
			So(err, ShouldBeNil)
			So(string(msg.Data), ShouldEqual, data)
			So(elapsed.Seconds(), ShouldBeGreaterThan, 1)

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
