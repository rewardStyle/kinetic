package kinetic

import (
	"encoding/binary"
	"errors"
	"runtime"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestProducerStop(t *testing.T) {
	producerInterface, _ := new(KinesisProducer).Init()
	producerInterface.NewEndpoint(testEndpoint, "stream-name")
	producer := producerInterface.(*KinesisProducer)
	CreateAndWaitForStream(producer.client, "stream-name")

	Convey("Given a running producer", t, func() {
		go producer.produce()

		Convey("It should stop producing if sent an interrupt signal", func() {
			producer.interrupts <- syscall.SIGINT
			runtime.Gosched()

			// Wait for it to stop
			for {
				if !producer.IsProducing() {
					break
				}
			}

			So(producer.IsProducing(), ShouldEqual, false)
		})
	})

	producer.Close()
}

func TestSyncStop(t *testing.T) {
	producerInterface, _ := new(KinesisProducer).Init()
	producerInterface.NewEndpoint(testEndpoint, "stream-name")
	producer := producerInterface.(*KinesisProducer)
	CreateAndWaitForStream(producer.client, "stream-name")

	Convey("Given a running producer", t, func() {
		go producer.produce()
		runtime.Gosched()
		Convey("It should stop producing if sent an interrupt signal", func() {
			err := producer.CloseSync()
			So(err, ShouldBeNil)
			// Wait for it to stop
			So(producer.IsProducing(), ShouldEqual, false)
		})
	})

	producer.Close()
}

func TestProducerError(t *testing.T) {
	producerInterface, _ := new(KinesisProducer).Init()
	producerInterface.NewEndpoint(testEndpoint, "stream-name")
	producer := producerInterface.(*KinesisProducer)
	CreateAndWaitForStream(producer.client, "stream-name")

	Convey("Given a running producer", t, func() {
		go producer.produce()

		Convey("It should handle errors successfully", func() {
			producer.errors <- errors.New("All your base are belong to us")
			// Let the error propagate
			<-time.After(3 * time.Second)
			So(producer.getErrCount(), ShouldEqual, 1)
			So(producer.IsProducing(), ShouldEqual, true)
		})
	})

	producer.Close()
}

func TestProducerMessage(t *testing.T) {
	listener, _ := new(Listener).InitC("your-stream", "0", "LATEST", "accesskey", "secretkey", "us-east-1", 4)
	producer, _ := new(KinesisProducer).InitC("your-stream", "0", "LATEST", "accesskey", "secretkey", "us-east-1", 4)

	listener.NewEndpoint(testEndpoint, "your-stream")
	producer.NewEndpoint(testEndpoint, "your-stream")

	CreateAndWaitForStream(producer.(*KinesisProducer).client, "your-stream")

	for _, c := range cases {
		Convey("Given a valid message", t, func() {
			producer.Send(new(Message).Init(c.message, "test"))

			Convey("It should be passed on the queue without error", func() {
				msg, err := listener.Retrieve()
				if err != nil {
					t.Fatalf(err.Error())
				}

				So(string(msg.Value()), ShouldResemble, string(c.message))
			})
		})
	}

	listener.Close()
	producer.Close()
}

func TestProducerTryToSend(t *testing.T) {
	producer, _ := new(KinesisProducer).InitC("your-stream", "0", "LATEST", "accesskey", "secretkey", "us-east-1", 4)
	producer.NewEndpoint(testEndpoint, "your-stream")
	CreateAndWaitForStream(producer.(*KinesisProducer).client, "stream-name")

	producer.Close() // This is to make the test deterministic.  It stops producer from sending messages.
	runtime.Gosched()
	var totDropped int
	for i := 0; i < 5000; i++ {
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(i))
		if err := producer.TryToSend(new(Message).Init(b, "foo")); nil != err {
			totDropped++
		}
	}
	Convey("Given a producer", t, func() {
		Convey("TryToSend should drop messages when the queue is full", func() {
			So(totDropped, ShouldEqual, 1000)
			So(len(producer.(*KinesisProducer).messages), ShouldEqual, 4000)
		})
	})
}
