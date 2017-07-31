package kinetic

import (
	. "github.com/smartystreets/goconvey/convey"

	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func putRecord(l *Consumer, b []byte) (*string, error) {
	resp, err := l.reader.(*KinesisReader).client.PutRecord(&kinesis.PutRecordInput{
		Data:         b,
		PartitionKey: aws.String("dummy"),
		StreamName:   aws.String(l.reader.(*KinesisReader).stream),
	})
	if err != nil {
		return nil, err
	}
	return resp.SequenceNumber, nil
}

func TestConsumer(t *testing.T) {
	Convey("given a consumer", t, func() {
		k, err := NewKinetic(
			AwsConfigCredentials("some-access-key", "some-secret-key", "some-security-token"),
			AwsConfigRegion("some-region"),
			AwsConfigEndpoint("http://127.0.0.1:4567"),
		)
		So(k, ShouldNotBeNil)
		So(err, ShouldBeNil)


		stream := "some-consumer-stream"

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
		r, err := NewKinesisReader(k.Session.Config, stream, shards[0],
			KinesisReaderBatchSize(5),
			//KinesisReaderShardIterator(),
			KinesisReaderResponseReadTimeout(time.Second),
			KinesisReaderLogLevel(aws.LogOff),
		)
		So(r, ShouldNotBeNil)
		So(err, ShouldBeNil)

		l, err := NewConsumer(k.Session.Config, stream, shards[0],
			ConsumerReader(r),
			ConsumerQueueDepth(10),
			ConsumerConcurrency(10),
			ConsumerLogLevel(aws.LogOff),
			ConsumerStats(&NilConsumerStatsCollector{}),
		)
		So(l, ShouldNotBeNil)
		So(err, ShouldBeNil)

		Convey("given a kinesis reader", func() {
			Convey("check that the reader was initialized correctly", func() {
				So(l.reader, ShouldNotBeNil)
				So(l.reader, ShouldEqual, r)
			})

			r := l.reader.(*KinesisReader)

			Convey("check that the reader was initialized with the correct stream name", func() {
				So(r.stream, ShouldEqual, stream)
			})

			Convey("check that the reader was initialized with the correct shard", func() {
				So(r.shard, ShouldEqual, shards[0])
			})

			Convey("check that the kinesis client was initialized correctly", func() {
				So(r.client, ShouldNotBeNil)
			})
		})

		Convey("check that setting an empty shard iterator returns an error", func() {
			err := l.reader.(*KinesisReader).setNextShardIterator("")
			So(err, ShouldEqual, ErrEmptyShardIterator)
		})

		Convey("check that setting an empty sequence number returns an error", func() {
			err := l.reader.(*KinesisReader).setSequenceNumber("")
			So(err, ShouldEqual, ErrEmptySequenceNumber)
		})

		Convey("check that we can get the TRIM_HORIZON shard iterator", func() {
			err := l.reader.(*KinesisReader).ensureShardIterator()
			So(err, ShouldBeNil)
			So(l.reader.(*KinesisReader).nextShardIterator, ShouldNotBeEmpty)
		})

		Convey("check that we can retrieve records one by one", func() {
			data := []string{"foo", "bar"}
			for n, datum := range data {
				seq, err := putRecord(l, []byte(datum))
				So(err, ShouldBeNil)
				So(seq, ShouldNotBeNil)
				msg, err := l.Retrieve()
				So(err, ShouldBeNil)
				So(string(msg.Data), ShouldEqual, datum)
				Convey(fmt.Sprintf("check that iteration %d properly advanced the shard iterator", n), func() {
					So(l.reader.(*KinesisReader).shardIterator.shardIteratorType, ShouldEqual, "AT_SEQUENCE_NUMBER")
					So(l.reader.(*KinesisReader).shardIterator.sequenceNumber, ShouldEqual, *seq)
				})
			}
		})

		Convey("check that retrieve will block until record comes", func(c C) {
			start := time.Now()
			data := "hello"
			go func() {
				<-time.After(1 * time.Second)
				_, err := putRecord(l, []byte(data))
				c.So(err, ShouldBeNil)
			}()
			msg, err := l.Retrieve()
			elapsed := time.Since(start)
			Printf("(it blocked %f seconds)\n", elapsed.Seconds())
			So(err, ShouldBeNil)
			So(string(msg.Data), ShouldEqual, data)
			So(elapsed.Seconds(), ShouldBeGreaterThan, 1)
		})

		Convey("check that we can use a context to cancel the retrieve", func() {
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
			defer cancel()
			_, err := l.RetrieveWithContext(ctx)
			elapsed := time.Since(start)
			Printf("(it blocked %f seconds)\n", elapsed.Seconds())
			So(err, ShouldNotBeNil)
			So(err, ShouldHaveSameTypeAs, context.DeadlineExceeded)
			So(elapsed.Seconds(), ShouldBeGreaterThan, 1)
		})

		Convey("check that we can use a context to cancel the retrieve (again)", func() {
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
			defer cancel()
			_, err := l.RetrieveWithContext(ctx)
			elapsed := time.Since(start)
			Printf("(it blocked %f seconds)\n", elapsed.Seconds())
			So(err, ShouldNotBeNil)
			So(err, ShouldHaveSameTypeAs, context.DeadlineExceeded)
			So(elapsed.Seconds(), ShouldBeGreaterThan, 0.01)
		})

		Convey("check that retrieve still works with a canceller if a message comes before the deadline", func(c C) {
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			defer cancel()

			data := "goodbye"
			go func() {
				<-time.After(1 * time.Second)
				_, err := putRecord(l, []byte(data))
				c.So(err, ShouldBeNil)
			}()
			msg, err := l.RetrieveWithContext(ctx)
			So(err, ShouldBeNil)
			So(string(msg.Data), ShouldEqual, data)
		})

		Convey("check that retrieve properly blocks other retrieves and attempts to set the shard id", func(c C) {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.TODO(), 1000*time.Millisecond)
				defer cancel()
				_, err := l.RetrieveWithContext(ctx)
				c.So(err, ShouldNotBeNil)
				c.So(err, ShouldHaveSameTypeAs, context.DeadlineExceeded)
				wg.Done()
			}()
			<-time.After(10 * time.Millisecond)
			_, err := l.Retrieve()
			So(err, ShouldEqual, ErrAlreadyConsuming)
			wg.Wait()
		})

		Convey("check that listen and retrieve can not be called concurrently", func(c C) {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.TODO(), 1000*time.Millisecond)
				defer cancel()
				l.ListenWithContext(ctx, func(msg *Message, wg *sync.WaitGroup) error {
					defer wg.Done()
					return nil
				})
				wg.Done()
			}()
			<-time.After(10 * time.Millisecond)
			_, err := l.Retrieve()
			So(err, ShouldEqual, ErrAlreadyConsuming)
			wg.Wait()
		})

		// TODO: Move this test to kinesis_reader_test.go
		Convey("check that throttle mechanism prevents more than 5 calls to get records", func() {
			start := time.Now()
			secs := []float64{}
			for i := 1; i <= 6; i++ {
				start := time.Now()
				l.reader.GetRecord(context.TODO(), func(msg *Message, wg *sync.WaitGroup) error {
					defer wg.Done()

					return nil
				})
				secs = append(secs, time.Since(start).Seconds())
			}
			elapsed := time.Since(start).Seconds()
			So(elapsed, ShouldBeGreaterThan, 1)
			Printf("%f seconds total, (%v)", elapsed, secs)
		})

		Convey("check that retrievefn can deliver messages to the fn", func(c C) {
			called := false
			data := "retrieved"
			_, err := putRecord(l, []byte(data))
			So(err, ShouldBeNil)
			err = l.RetrieveFn(func(msg *Message, wg *sync.WaitGroup) error {
				defer wg.Done()

				called = true
				// Note that because this is called in a goroutine, we have to use
				// the goconvey context
				c.So(string(msg.Data), ShouldEqual, data)

				return nil
			})
			So(err, ShouldBeNil)
			So(called, ShouldBeTrue)
		})

		Convey("check that listen can deliver messages to fn", func(c C) {
			planets := []string{"mercury", "venus", "earth", "mars", "jupiter", "saturn", "neptune", "uranus"}
			var count int64
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				l.Listen(func(msg *Message, wg *sync.WaitGroup) error {
					defer wg.Done()
					atomic.AddInt64(&count, 1)

					return nil
				})
			}()
			for _, planet := range planets {
				_, err := putRecord(l, []byte(planet))
				So(err, ShouldBeNil)
			}
			timeout := time.After(10 * time.Second)
			// FIXME: Not too thrilled with this implementation, but
			// there is probably a race condition between when the
			// last planet is put onto the Kinesis stream (and
			// subsequently read by consume) with when closing the
			// pipeOfDeath (which will shut down the consume loop)
			// such that we may not see all the planets inside
			// Listen.
		stop:
			for {
				select {
				case <-time.After(1 * time.Second):
					if atomic.LoadInt64(&count) == int64(len(planets)) {
						break stop
					}
				case <-timeout:
					break stop
				}
			}
			// FIXME: probably a race condition here as consume may
			// not have grabbed all data from the channel yet.
			close(l.pipeOfDeath)
			wg.Wait()
			So(atomic.LoadInt64(&count), ShouldEqual, len(planets))
		})

		Convey("check that listen can be cancelled by context", func(c C) {
			for i := 0; i < 20; i++ {
				_, err := putRecord(l, []byte(fmt.Sprintf("%d", i)))
				So(err, ShouldBeNil)
			}
			var count int64
			ctx, cancel := context.WithCancel(context.TODO())
			go func() {
				l.ListenWithContext(ctx, func(m *Message, wg *sync.WaitGroup) error {
					defer wg.Done()
					time.AfterFunc(time.Duration(rand.Intn(3))*time.Second, func() {
						n, err := strconv.Atoi(string(m.Data))
						c.So(n, ShouldBeBetweenOrEqual, 0, 19)
						c.So(err, ShouldBeNil)
						atomic.AddInt64(&count, 1)
					})

					return nil
				})
			}()
			<-time.After(1 * time.Second)
			cancel()
			So(atomic.LoadInt64(&count), ShouldBeBetweenOrEqual, 1, 20)
			Printf("(count was %d)", atomic.LoadInt64(&count))
		})

		Reset(func() {
			k.DeleteStream(stream)
			k.WaitUntilStreamDeleted(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		})
	})
}
