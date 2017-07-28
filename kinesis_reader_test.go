package kinetic

import (
	. "github.com/smartystreets/goconvey/convey"

	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

func TestNewKinesisReader(t *testing.T) {
	// Set the RNG Seed based on current time (in order to randomize the RNG)
	rand.Seed(time.Now().UTC().UnixNano())

	Convey("given a kinetic object and kinesis stream/shard names", t, func() {
		// Instantiate a new kinentic object
		k, err := NewKinetic(
			AwsConfigCredentials("some-access-key", "some-secret-key", "some-security-token"),
			AwsConfigRegion("some-region"),
			AwsConfigEndpoint("http://127.0.0.1:4567"),
		)
		So(k, ShouldNotBeNil)
		So(err, ShouldBeNil)

		stream := "some-stream"
		shard := "some-shard"

		Convey("check that we can create a new KinesisReader with default values", func() {
			r, err := NewKinesisReader(k.Session.Config, stream, shard)
			So(r, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(r.batchSize, ShouldEqual, 10000)
			So(r.shardIterator, ShouldNotBeNil)
			So(r.responseReadTimeout, ShouldEqual, time.Second)
			So(r.Stats, ShouldNotBeNil)
		})

		Convey("check that we can create a new KinesisReader with configured values", func() {
			batchSize := rand.Intn(kinesisReaderBatchSize)
			respReadTimeout := time.Duration(rand.Int()) * time.Second
			logLevel := aws.LogDebug | aws.LogDebugWithSigning | LogDebug
			shardIterator := NewShardIterator()
			myStatsCollector := &NilConsumerStatsCollector{}
			r, err := NewKinesisReader(k.Session.Config, stream, shard,
				KinesisReaderBatchSize(batchSize),
				KinesisReaderShardIterator(shardIterator),
				KinesisReaderResponseReadTimeout(respReadTimeout),
				KinesisReaderLogLevel(logLevel),
				KinesisReaderStats(myStatsCollector),
			)
			So(r, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(r.batchSize, ShouldEqual, batchSize)
			So(r.responseReadTimeout, ShouldEqual, respReadTimeout)
			So(r.LogLevel.AtLeast(LogDebug), ShouldBeTrue)
			So(r.shardIterator, ShouldEqual, shardIterator)
			So(r.Stats, ShouldEqual, myStatsCollector)
		})
	})
}
