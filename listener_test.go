package kinetic

import (
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestListenerStop(t *testing.T) {
	listener, err := new(Listener).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	Convey("Given a running listener", t, func() {
		go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
			wg.Done()
		})

		Convey("It should stop listening if sent an interrupt signal", func() {
			listener.interrupts <- syscall.SIGINT
			So(listener.IsListening(), ShouldEqual, false)
		})
	})

	listener.Close()
}

// This test has a race condition due to running Listen in a goroutine
func TestListenerError(t *testing.T) {
	listener, err := new(Listener).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	Convey("Given a running listener", t, func() {
		go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
			wg.Done()
		})

		Convey("It should handle errors successfully", func() {
			listener.errors <- errors.New("All your base are belong to us!")

			// Let the error propagate
			<-time.After(1 * time.Second)

			So(listener.errCount, ShouldEqual, 1)
			So(listener.IsListening(), ShouldEqual, true)
		})
	})

	listener.Close()
}

func TestListenerMessage(t *testing.T) {
	listener, err := new(Listener).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
		wg.Done()
	})

	for _, c := range cases {
		Convey("Given a running listener", t, func() {
			listener.messages <- new(KinesisMessage).Init(c.message, "test")
			Convey("It should handle messages successfully", func() {
				So(listener.IsListening(), ShouldEqual, true)
				So(listener.Errors(), ShouldNotResemble, nil)
			})
		})
	}

	listener.Close()
}

func TestRetrieveMessage(t *testing.T) {
	listener, err := new(Listener).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	producer, err := new(Producer).Init(conf.Kinesis.Stream, conf.Kinesis.Shard)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for _, c := range cases {
		Convey("Given a valid message", t, func() {
			producer.Send(new(KinesisMessage).Init(c.message, "test"))

			Convey("It should be passed on the queue without error", func() {
				msg, err := listener.Retrieve()
				if err != nil {
					t.Fatalf(err.Error())
				}

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
