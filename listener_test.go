package kinetic

import (
	"errors"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsKinesis "github.com/aws/aws-sdk-go/service/kinesis"
	awsKinesisIface "github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	. "github.com/smartystreets/goconvey/convey"
)

const testEndpoint = "http://127.0.0.1:4567"

func CreateAndWaitForStream(client awsKinesisIface.KinesisAPI, name string) {
	client.CreateStream(&awsKinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int64(1),
	})
	stream := &awsKinesis.DescribeStreamInput{StreamName: aws.String(name), Limit: aws.Int64(1)}
	client.WaitUntilStreamExists(stream)
}

func TestListenerStop(t *testing.T) {
	listener, _ := new(Listener).Init()
	listener.NewEndpoint(testEndpoint, "stream-name")
	CreateAndWaitForStream(listener.client, "stream-name")
	listener.ReInit()

	Convey("Given a running listener", t, func() {
		go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
			wg.Done()
		})

		Convey("It should stop listening if sent an interrupt signal", func() {
			listener.interrupts <- syscall.SIGINT
			runtime.Gosched()
			// Let it finish stopping
			time.Sleep(3 * time.Second)

			So(listener.IsListening(), ShouldEqual, false)
		})
	})

	listener.Close()
}

func TestListenerSyncStop(t *testing.T) {
	listener, _ := new(Listener).Init()
	listener.NewEndpoint(testEndpoint, "stream-name")
	CreateAndWaitForStream(listener.client, "stream-name")
	listener.ReInit()

	Convey("Given a running listener", t, func() {
		go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
			wg.Done()
		})

		Convey("It should stop listening if sent an interrupt signal", func() {
			err := listener.CloseSync()
			So(err, ShouldBeNil)
			So(listener.IsListening(), ShouldEqual, false)
		})
	})

	listener.Close()
}

func TestListenerError(t *testing.T) {
	listener, _ := new(Listener).Init()
	listener.NewEndpoint(testEndpoint, "stream-name")
	CreateAndWaitForStream(listener.client, "stream-name")
	listener.ReInit()

	Convey("Given a running listener", t, func() {
		go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
			wg.Done()
		})

		Convey("It should handle errors successfully", func() {
			listener.errors <- errors.New("All your base are belong to us")

			// Let the error propagate
			<-time.After(3 * time.Second)

			So(listener.getErrCount(), ShouldNotEqual, 0)
			So(listener.IsListening(), ShouldEqual, true)
		})
	})

	listener.Close()
}

func TestListenerMessage(t *testing.T) {
	listener, _ := new(Listener).Init()
	listener.NewEndpoint(testEndpoint, "stream-name")
	CreateAndWaitForStream(listener.client, "stream-name")
	listener.ReInit()

	go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
		wg.Done()
	})

	<-time.After(3 * time.Second)

	for _, c := range cases {
		Convey("Given a running listener", t, func() {
			listener.messages <- new(Message).Init(c.message, "test")

			Convey("It should handle messages successfully", func() {
				So(listener.IsListening(), ShouldEqual, true)
				So(listener.Errors(), ShouldNotResemble, nil)
			})
		})
	}

	listener.Close()
}

func TestRetrieveMessage(t *testing.T) {
	listener, _ := new(Listener).InitC("your-stream", "0", ShardIterTypes[3], "accesskey", "secretkey", "us-east-1", 10)
	producer, _ := new(KinesisProducer).InitC("your-stream", "0", ShardIterTypes[3], "accesskey", "secretkey", "us-east-1", 10)

	listener.NewEndpoint(testEndpoint, "your-stream")
	producer.NewEndpoint(testEndpoint, "your-stream")
	CreateAndWaitForStream(listener.client, "your-stream")
	listener.ReInit()
	producer.ReInit()

	time.Sleep(10 * time.Millisecond)
	for _, c := range cases {
		Convey("Given a valid message", t, func() {
			producer.Send(new(Message).Init(c.message, "test"))
			time.Sleep(3 * time.Millisecond)

			Convey("It should be passed on the queue without error", func() {
				msg, err := listener.Retrieve()
				// if err != nil {
				// 	t.Fatalf(err.Error())
				// }
				So(err, ShouldBeNil)

				So(string(msg.Value()), ShouldResemble, string(c.message))
			})
		})
	}

	producer.Close()
	listener.Close()
}

var cases = []struct {
	message []byte
}{
	{
		message: []byte(`{"foo":"bar"}`),
	},
	{
		message: []byte(`{"bar":"baz"}`),
	},
	{
		message: []byte(`{"baz":"qux"}`),
	},
}
