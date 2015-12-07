package kinetic

import (
	"errors"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestProducerStop(t *testing.T) {
	producer, err := new(Producer).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	producer.SetTestEndpoint(testEndpoint)

	Convey("Given a running producer", t, func() {
		go producer.produce()

		Convey("It should stop producing if sent an interrupt signal", func() {
			producer.interrupts <- syscall.SIGINT

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

func TestProducerError(t *testing.T) {
	producer, err := new(Producer).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	producer.SetTestEndpoint(testEndpoint)

	Convey("Given a running producer", t, func() {
		go producer.produce()

		Convey("It should handle errors successfully", func() {
			producer.errors <- errors.New("All your base are belong to us!")
			// Let the error propagate
			<-time.After(1 * time.Second)
			So(producer.errCount, ShouldEqual, 1)
			So(producer.IsProducing(), ShouldEqual, true)
		})
	})

	producer.Close()
}

func TestProducerMessage(t *testing.T) {
	listener, err := new(Listener).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	producer, err := new(Producer).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	listener.SetTestEndpoint(testEndpoint)
	producer.SetTestEndpoint(testEndpoint)

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
