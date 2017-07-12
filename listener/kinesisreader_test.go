package listener

import (
	. "github.com/smartystreets/goconvey/convey"

	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/logging"
)

func TestNewKinesisReader(t *testing.T) {
	// Set the RNG Seed based on current time (in order to randomize the RNG)
	rand.Seed(time.Now().UTC().UnixNano())

	Convey("given a kinetic object and kinesis stream/shard names", t, func() {
		// Instantiate a new kinentic object
		k, err := kinetic.New(func(c *kinetic.Config) {
			c.SetCredentials("some-access-key", "some-secret-key", "some-security-token")
			c.SetRegion("some-region")
			c.SetEndpoint("http://127.0.0.1:4567")
		})
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
			batchSize := rand.Int()
			respReadTimeout := time.Duration(rand.Int()) * time.Second
			logLevel := aws.LogDebug | aws.LogDebugWithSigning | logging.LogDebug
			shardIterator := NewShardIterator()
			myStatsCollector := &NilStatsCollector{}
			r, err := NewKinesisReader(k.Session.Config, stream, shard, func(krc *KinesisReaderConfig) {
				krc.SetBatchSize(batchSize)
				krc.SetResponseReadTimeout(respReadTimeout)
				krc.SetLogLevel(logLevel)
				krc.SetInitialShardIterator(shardIterator)
				krc.SetStatsCollector(myStatsCollector)
			})
			So(r, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(r.batchSize, ShouldEqual, batchSize)
			So(r.responseReadTimeout, ShouldEqual, respReadTimeout)
			So(r.LogLevel.AtLeast(logging.LogDebug), ShouldBeTrue)
			So(r.shardIterator, ShouldEqual, shardIterator)
			So(r.Stats, ShouldEqual, myStatsCollector)
		})
	})
}
