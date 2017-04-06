package listener

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

	"github.com/rewardStyle/kinetic"
)

func putRecord(l *Listener, b []byte) (*string, error) {
	l.ensureClient()
	resp, err := l.client.PutRecord(&kinesis.PutRecordInput{
		Data:         b,
		PartitionKey: aws.String("dummy"),
		StreamName:   aws.String(l.stream),
	})
	if err != nil {
		return nil, err
	}
	return resp.SequenceNumber, nil
}

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

func TestListener(t *testing.T) {
	Convey("given a listener", t, func() {
		k, err := kinetic.New(func(c *kinetic.Config) {
			c.SetCredentials("some-access-key", "some-secret-key", "some-security-token")
			c.SetRegion("some-region")
			c.SetEndpoint("http://127.0.0.1:4567")
		})

		stream := "some-listener-stream"

		err = k.CreateStream(stream, 1)
		So(err, ShouldBeNil)

		err = k.WaitUntilStreamExists(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		So(err, ShouldBeNil)

		shards, err := k.GetShards(stream)
		So(err, ShouldBeNil)
		So(len(shards), ShouldEqual, 1)

		l, err := NewListener(stream, shards[0], func(c *Config) {
			c.FromKinetic(k)
			c.SetConcurrency(10)
		})
		So(l, ShouldNotBeNil)
		So(err, ShouldBeNil)

		Convey("check that calling ensureClient twice doesn't overwrite existing client", func() {
			So(l.client, ShouldBeNil)
			l.ensureClient()
			So(l.client, ShouldNotBeNil)
			client := l.client
			l.ensureClient()
			So(l.client, ShouldEqual, client)
		})

		Convey("check that setting an empty shard iterator returns an error", func() {
			err := l.setNextShardIterator("")
			So(err, ShouldEqual, ErrEmptyShardIterator)
		})

		Convey("check that setting an empty sequence number returns an error", func() {
			err := l.setSequenceNumber("")
			So(err, ShouldEqual, ErrEmptySequenceNumber)
		})

		Convey("check that we can get the TRIM_HORIZON shard iterator", func() {
			err := l.ensureShardIterator()
			So(err, ShouldBeNil)
			So(l.nextShardIterator, ShouldNotBeEmpty)
		})

		Convey("check that we can retrieve records one by one", func() {
			data := []string{"foo", "bar"}
			for n, datum := range data {
				seq, err := putRecord(l, []byte(datum))
				So(err, ShouldBeNil)
				So(seq, ShouldNotBeNil)
				msg, err := l.Retrieve()
				So(err, ShouldBeNil)
				So(string(msg.Value()), ShouldEqual, datum)
				Convey(fmt.Sprintf("check that iteration %d properly advanced the shard iterator", n), func() {
					So(l.shardIterator.shardIteratorType, ShouldEqual, "AT_SEQUENCE_NUMBER")
					So(l.shardIterator.sequenceNumber, ShouldEqual, *seq)
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
			So(string(msg.Value()), ShouldEqual, data)
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
			So(string(msg.Value()), ShouldEqual, data)
		})

		Convey("check that retrieve properly blocks other retrieves and attempts to set the shard id", func(c C) {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
				defer cancel()
				_, err := l.RetrieveWithContext(ctx)
				c.So(err, ShouldNotBeNil)
				c.So(err, ShouldHaveSameTypeAs, context.DeadlineExceeded)
				wg.Done()
			}()
			<-time.After(10 * time.Millisecond)
			So(l.IsConsuming(), ShouldBeTrue)
			_, err := l.Retrieve()
			So(err, ShouldEqual, ErrAlreadyConsuming)
			wg.Wait()
		})

		Convey("check that throttle mechanism prevents more than 5 calls to get records", func() {
			start := time.Now()
			secs := []float64{}
			for i := 1; i <= 6; i++ {
				start := time.Now()
				l.getRecords(1)
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
			err = l.RetrieveFn(func(b []byte, wg *sync.WaitGroup) {
				called = true
				// Note that because this is called in a goroutine, we have to use
				// the goconvey context
				c.So(string(b), ShouldEqual, data)
				wg.Done()
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
				l.Listen(func(b []byte, wg *sync.WaitGroup) {
					defer wg.Done()
					atomic.AddInt64(&count, 1)
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
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithCancel(context.TODO())
				defer wg.Done()
				l.ListenWithContext(ctx, func(b []byte, wg *sync.WaitGroup) {
					defer wg.Done()
					time.AfterFunc(time.Duration(rand.Intn(10))*time.Second, func() {
						n, err := strconv.Atoi(string(b))
						c.So(err, ShouldBeNil)
						atomic.AddInt64(&count, 1)
						if n == 15 {
							cancel()
						}
					})
				})
			}()
			wg.Wait()
			So(atomic.LoadInt64(&count), ShouldBeBetweenOrEqual, 1, 20)
			Printf("(count was %d)", atomic.LoadInt64(&count))
		})

		Reset(func() {
			k.DeleteStream(stream)
			k.WaitUntilStreamDeleted(context.TODO(), stream, request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		})

	})
}
