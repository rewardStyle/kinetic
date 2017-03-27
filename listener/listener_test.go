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
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type DebugLogger struct {
	logs []string
}

func (l *DebugLogger) Log(args ...interface{}) {
	l.logs = append(l.logs, fmt.Sprint(args...))
}

func (l *DebugLogger) Reset() {
	l.logs = nil
}

type DebugStatsListener struct{}

func (l *DebugStatsListener) AddConsumedSample(int)                       {}
func (l *DebugStatsListener) AddDeliveredSample(int)                      {}
func (l *DebugStatsListener) AddProcessedSample(int)                      {}
func (l *DebugStatsListener) AddBatchSizeSample(int)                      {}
func (l *DebugStatsListener) AddGetRecordsCalled(int)                     {}
func (l *DebugStatsListener) AddProvisionedThroughputExceeded(int)        {}
func (l *DebugStatsListener) AddGetRecordsTimeout(int)                    {}
func (l *DebugStatsListener) AddGetRecordsReadTimeout(int)                {}
func (l *DebugStatsListener) AddGetRecordsReadResponseTime(time.Duration) {}
func (l *DebugStatsListener) AddGetRecordsUnmarshalTime(time.Duration)    {}

func getSession(config *Config) *session.Session {
	sess, err := config.GetAwsSession()
	So(err, ShouldBeNil)
	So(sess, ShouldNotBeNil)
	return sess
}

func putRecord(l *Listener, b []byte) (*string, error) {
	resp, err := l.client.PutRecord(&kinesis.PutRecordInput{
		Data:         b,
		PartitionKey: aws.String("dummy"),
		StreamName:   aws.String(l.Config.stream),
	})
	if err != nil {
		return nil, err
	}
	return resp.SequenceNumber, nil
}

func TestNewConfig(t *testing.T) {
	Convey("given a new listener config", t, func() {
		config := NewConfig("some-stream")

		Convey("check the default values for its non-zero config", func() {
			So(config.awsConfig.HTTPClient.Timeout, ShouldEqual, 5*time.Minute)
			So(config.stream, ShouldEqual, "some-stream")
			So(config.logLevel, ShouldEqual, aws.LogOff)
			So(config.stats, ShouldHaveSameTypeAs, &NilStatsListener{})
			So(config.batchSize, ShouldEqual, 10000)
			So(config.concurrency, ShouldEqual, 10000)
			So(config.shardIterator.shardIteratorType, ShouldEqual, "TRIM_HORIZON")
			So(config.getRecordsReadTimeout, ShouldEqual, 1*time.Second)
		})

		Convey("check that we can retrieve an aws.Session from it ", func() {
			getSession(config)
		})

		Convey("check that we can set credentials", func() {
			config = config.WithCredentials("access-key", "secret-key", "security-token")
			sess := getSession(config)
			creds, err := sess.Config.Credentials.Get()
			So(err, ShouldBeNil)
			So(creds.AccessKeyID, ShouldEqual, "access-key")
			So(creds.SecretAccessKey, ShouldEqual, "secret-key")
			So(creds.SessionToken, ShouldEqual, "security-token")
		})

		Convey("check that we can set the region", func() {
			config = config.WithRegion("my-region")
			sess := getSession(config)
			So(aws.StringValue(sess.Config.Region), ShouldEqual, "my-region")
		})

		Convey("check that we can set the endpoint", func() {
			config = config.WithEndpoint("my-endpoint")
			sess := getSession(config)
			So(aws.StringValue(sess.Config.Endpoint), ShouldEqual, "my-endpoint")
		})

		Convey("check that we can configure a logger", func() {
			l := &DebugLogger{}
			config = config.WithLogger(l)
			sess := getSession(config)
			So(sess.Config.Logger, ShouldHaveSameTypeAs, l)

			Convey("check that basic logging should work", func() {
				sess.Config.Logger.Log("one")
				sess.Config.Logger.Log("two")
				sess.Config.Logger.Log("three")
				So(len(l.logs), ShouldEqual, 3)
				So(l.logs, ShouldContain, "one")
				So(l.logs, ShouldContain, "two")
				So(l.logs, ShouldContain, "three")
				Reset(func() {
					l.Reset()
				})
			})
		})

		Convey("check that the default log level is off for both the sdk and kinetic", func() {
			sess := getSession(config)
			So(sess.Config.LogLevel.Value(), ShouldEqual, aws.LogOff)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeFalse)
			So(config.logLevel.Value(), ShouldEqual, aws.LogOff)
			So(config.logLevel.AtLeast(aws.LogDebug), ShouldBeFalse)
		})

		Convey("check that we can set the sdk log level", func() {
			config = config.WithLogLevel(aws.LogDebug | aws.LogDebugWithSigning)
			sess := getSession(config)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeTrue)
			So(sess.Config.LogLevel.Matches(aws.LogDebugWithSigning), ShouldBeTrue)
			So(config.logLevel.Value(), ShouldEqual, aws.LogOff)
			So(config.logLevel.AtLeast(aws.LogDebug), ShouldBeFalse)
		})

		Convey("check that we can set the kinetic log level", func() {
			config = config.WithLogLevel((aws.LogDebug | 1) << 16)
			sess := getSession(config)
			So(sess.Config.LogLevel.Value(), ShouldEqual, aws.LogOff)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeFalse)
			So(config.logLevel.AtLeast(aws.LogDebug), ShouldBeTrue)
			So(config.logLevel.Matches(1), ShouldBeTrue)
		})

		Convey("check that we can set both the sdk and kinetic log level", func() {
			config = config.WithLogLevel(aws.LogDebug | aws.LogDebugWithSigning | ((aws.LogDebug | 1) << 16))
			sess := getSession(config)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeTrue)
			So(sess.Config.LogLevel.Matches(aws.LogDebugWithSigning), ShouldBeTrue)
			So(config.logLevel.AtLeast(aws.LogDebug), ShouldBeTrue)
			So(config.logLevel.Matches(1), ShouldBeTrue)
		})

		Convey("check that we can set the http.Client Timeout", func() {
			config = config.WithHttpClientTimeout(10 * time.Minute)
			So(config.awsConfig.HTTPClient.Timeout, ShouldEqual, 10*time.Minute)
		})

		Convey("check that we can configure a stats listener", func() {
			config = config.WithStatsListener(&DebugStatsListener{})
			So(config.stats, ShouldHaveSameTypeAs, &DebugStatsListener{})
		})

		Convey("check that we can set the shard", func() {
			config = config.WithShardId("some-shard")
			So(config.shard, ShouldEqual, "some-shard")
		})

		Convey("check that we can set the batch size", func() {
			config = config.WithBatchSize(1000)
			So(config.batchSize, ShouldEqual, 1000)
		})

		Convey("check that we can set the concurrency limit", func() {
			config = config.WithConcurrency(50)
			So(config.concurrency, ShouldEqual, 50)
		})

		Convey("check that the default shard iterator is TRIM_HORIZON", func() {
			config = config.WithInitialShardIterator(NewShardIterator())
			So(config.shardIterator.shardIteratorType, ShouldEqual, "TRIM_HORIZON")
			So(config.shardIterator.getStartingSequenceNumber(), ShouldBeNil)
			So(config.shardIterator.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can set the initial shard iterator (to LATEST)", func() {
			config = config.WithInitialShardIterator(NewShardIterator().Latest())
			So(config.shardIterator.shardIteratorType, ShouldEqual, "LATEST")
			So(config.shardIterator.getStartingSequenceNumber(), ShouldBeNil)
			So(config.shardIterator.getTimestamp(), ShouldBeNil)
		})

		Convey("check that we can set the read timeout for the GetRecords request", func() {
			config = config.WithGetRecordsReadTimeout(10 * time.Second)
			So(config.getRecordsReadTimeout, ShouldEqual, 10*time.Second)
		})
	})
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
		logger := &DebugLogger{}
		l, err := NewListener(NewConfig("some-stream").
			WithRegion("some-region").
			WithEndpoint("http://127.0.0.1:4567").
			WithLogger(logger).
			WithLogLevel(aws.LogDebug << 16).
			WithConcurrency(10))
		So(l, ShouldNotBeNil)
		So(err, ShouldBeNil)

		Convey("check that logging works", func() {
			l.Log("foo")
			So(logger.logs, ShouldContain, "foo")
			So(len(logger.logs), ShouldEqual, 1)
			Reset(func() { logger.Reset() })
		})

		Convey("check that calling ensureClient twice doesn't overwrite existing client", func() {
			So(l.client, ShouldBeNil)
			l.ensureClient()
			So(l.client, ShouldNotBeNil)
			client := l.client
			l.ensureClient()
			So(l.client, ShouldEqual, client)
		})

		Convey("check deleting a non-existent stream returns an error", func() {
			err := l.DeleteStream()
			So(err, ShouldNotBeNil)
			e := err.(awserr.Error)
			So(e.Code(), ShouldEqual, kinesis.ErrCodeResourceNotFoundException)
		})

		Convey("check that setting an empty shard iterator returns an error", func() {
			err := l.setNextShardIterator("")
			So(err, ShouldEqual, ErrEmptyShardIterator)
		})

		Convey("check that setting an empty sequence number returns an error", func() {
			err := l.setSequenceNumber("")
			So(err, ShouldEqual, ErrEmptySequenceNumber)
		})

		Convey("check getting shards on a non-existent stream returns an error", func() {
			shards, err := l.GetShards()
			So(shards, ShouldBeNil)
			So(err, ShouldNotBeNil)
			e := err.(awserr.Error)
			So(e.Code(), ShouldEqual, kinesis.ErrCodeResourceNotFoundException)
		})

		Convey("check that we can create a stream", func() {
			err := l.CreateStream(1)
			So(err, ShouldBeNil)
			err = l.WaitUntilActive(context.TODO(), request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
			So(err, ShouldBeNil)

			Convey("check that creating an existing stream returns an error", func() {
				err := l.CreateStream(1)
				So(err, ShouldNotBeNil)
				e := err.(awserr.Error)
				So(e.Code(), ShouldEqual, kinesis.ErrCodeResourceInUseException)
			})

			Convey("check that getting the shard iterator with an empty shard returns an error", func() {
				So(l.Config.shard, ShouldBeEmpty)
				err := l.ensureShardIterator()
				So(err, ShouldNotBeNil)
				e := err.(awserr.Error)
				So(e.Code(), ShouldEqual, request.InvalidParameterErrCode)
			})

			Convey("check that getting the shard iterator without an invalid shard returns an error", func() {
				l.SetShard("some-shard")
				err := l.ensureShardIterator()
				So(err, ShouldNotBeNil)
				e := err.(awserr.Error)
				So(e.Code(), ShouldEqual, kinesis.ErrCodeResourceNotFoundException)
			})

			Convey("check that we can obtain a list of shards", func() {
				shards, err := l.GetShards()
				So(err, ShouldBeNil)
				So(len(shards), ShouldEqual, 1)

				Convey("check that we can set the shard id", func() {
					l.SetShard(shards[0])
					So(l.Config.shard, ShouldEqual, shards[0])

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
								So(l.Config.shardIterator.shardIteratorType, ShouldEqual, "AT_SEQUENCE_NUMBER")
								So(l.Config.shardIterator.sequenceNumber, ShouldEqual, *seq)
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
						ctx, _ := context.WithTimeout(context.TODO(), 1*time.Second)
						_, err := l.RetrieveWithContext(ctx)
						elapsed := time.Since(start)
						Printf("(it blocked %f seconds)\n", elapsed.Seconds())
						So(err, ShouldNotBeNil)
						So(err, ShouldHaveSameTypeAs, context.DeadlineExceeded)
						So(elapsed.Seconds(), ShouldBeGreaterThan, 1)
					})

					Convey("check that we can use a context to cancel the retrieve (again)", func() {
						start := time.Now()
						ctx, _ := context.WithTimeout(context.TODO(), 10*time.Millisecond)
						_, err := l.RetrieveWithContext(ctx)
						elapsed := time.Since(start)
						Printf("(it blocked %f seconds)\n", elapsed.Seconds())
						So(err, ShouldNotBeNil)
						So(err, ShouldHaveSameTypeAs, context.DeadlineExceeded)
						So(elapsed.Seconds(), ShouldBeGreaterThan, 0.01)
					})

					Convey("check that retrieve still works with a canceller if a message comes before the deadline", func(c C) {
						ctx, _ := context.WithTimeout(context.TODO(), 5*time.Second)

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
							ctx, _ := context.WithTimeout(context.TODO(), 1*time.Second)
							_, err := l.RetrieveWithContext(ctx)
							c.So(err, ShouldNotBeNil)
							c.So(err, ShouldHaveSameTypeAs, context.DeadlineExceeded)
							wg.Done()
						}()
						<-time.After(10 * time.Millisecond)
						So(l.IsConsuming(), ShouldBeTrue)
						_, err := l.Retrieve()
						So(err, ShouldEqual, ErrAlreadyConsuming)
						err = l.SetShard("bogus")
						So(err, ShouldEqual, ErrCannotSetShard)
						wg.Wait()
					})

					Convey("check that throttle mechanism prevents more than 5 calls to get records", func() {
						start := time.Now()
						secs := []float64{}
						for i := 1; i <= 6; i++ {
							start := time.Now()
							l.fetchBatch(1)
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
						var count int64 = 0
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
						close(l.pipeOfDeath)
						wg.Wait()
						So(count, ShouldEqual, len(planets))
					})

					Convey("check that listen can be cancelled by context", func(c C) {
						for i := 0; i < 20; i++ {
							_, err := putRecord(l, []byte(fmt.Sprintf("%d", i)))
							So(err, ShouldBeNil)
						}
						var count int64 = 0
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
						So(count, ShouldBeBetweenOrEqual, 1, 20)
						Printf("(count was %d)", count)
					})
				})

			})

			Reset(func() {
				l.DeleteStream()
				l.WaitUntilDeleted(context.TODO(), request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
			})
		})
	})
}
