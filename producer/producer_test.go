package producer

import (
	. "github.com/smartystreets/goconvey/convey"

	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"

	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/consumer"
	"github.com/rewardStyle/kinetic/producer"
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
			request.WithWaiterDelay(request.ConstantWaiterDelay(time.Second)))
		So(err, ShouldBeNil)

		shards, err := k.GetShards(stream)
		So(err, ShouldBeNil)
		So(len(shards), ShouldEqual, 1)

		So(k.Session, ShouldNotBeNil)
		So(k.Session.Config, ShouldNotBeNil)
		w, err := NewKinesisWriter(k.Session.Config, stream)
		So(w, ShouldNotBeNil)
		So(err, ShouldBeNil)

		p, err := NewProducer(k.Session.Config, w,
			producer.ProducerBatchSize(5),
			producer.ProducerBatchTimeout(time.Second),
			producer.ProducerMaxRetryAttempts(3),
			producer.ProducerQueueDepth(10),
			producer.ProducerConcurrency(2),
			producer.ProducerShardCheckFrequency(time.Minute),
			producer.ProducerDataSpillFn(func(msg *kinetic.Message) error {
				//log.Printf("Message was dropped: [%s]\n", string(msg.Data))
				return nil
			}),
			producer.ProducerLogLevel(aws.LogOff),
			//producer.ProducerStatsCollector(),
		)
		So(p, ShouldNotBeNil)
		So(err, ShouldBeNil)

		So(k.Session, ShouldNotBeNil)
		So(k.Session.Config, ShouldNotBeNil)
		r, err := consumer.NewKinesisReader(k.Session.Config, stream, shards[0],
			//consumer.KinesisReaderBatchSize(),
			//consumer.KinesisReaderShardIterator(),
			consumer.KinesisReaderResponseReadTimeout(time.Second),
			//consumer.KinesisReaderLogLevel(),
			//consumer.KinesisReaderStatsCollector(),
		)
		So(r, ShouldNotBeNil)
		So(err, ShouldBeNil)

		l, err := consumer.NewConsumer(k.Session.Config, r,
			consumer.ConsumerQueueDepth(10),
			consumer.ConsumerConcurrency(10),
			consumer.ConsumerLogLevel(aws.LogOff),
			//consumer.ConsumerStatsCollector(lsc),
		)
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

		Convey("check that we can send and receive a single message", func() {
			start := time.Now()
			data := "hello"
			p.Send(&kinetic.Message{
				PartitionKey: aws.String("key"),
				Data:         []byte(data),
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
			p.Send(&kinetic.Message{
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

		Convey("check that we can send a batch of messages after batch size is reached", func(c C) {
			start := time.Now()
			var elapsed time.Duration
			data := []string{"hello1", "hello2", "hello3", "hello4", "hello5", "hello6"}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					msg, err := l.Retrieve()
					c.So(err, ShouldBeNil)
					c.So(string(msg.Data), ShouldEqual, data[i])
				}
				elapsed = time.Since(start)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, datum := range data {
					p.Send(&kinetic.Message{
						PartitionKey: aws.String("key"),
						Data:         []byte(datum),
					})
				}
			}()
			wg.Wait()

			//So(elapsed.Seconds(), ShouldBeLessThan, 1)
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
