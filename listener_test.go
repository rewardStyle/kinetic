package kinetic

import (
	"errors"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const testEndpoint = "http://127.0.0.1:4567"

func TestListenerStop(t *testing.T) {
	listener, _ := new(Listener).Init()
	listener.NewEndpoint(testEndpoint, "stream-name")

	Convey("Given a running listener", t, func() {
		go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
			wg.Done()
		})

		Convey("It should stop listening if sent an interrupt signal", func() {
			listener.interrupts <- syscall.SIGINT
			runtime.Gosched()
			// Let it finish stopping
			<-time.After(3 * time.Second)

			So(listener.IsListening(), ShouldEqual, false)
		})
	})

	listener.Close()
}

func TestListenerError(t *testing.T) {
	listener, _ := new(Listener).Init()
	listener.NewEndpoint(testEndpoint, "stream-name")

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

	go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
		wg.Done()
	})

	<-time.After(3 * time.Second)

	for _, c := range cases {
		Convey("Given a running listener", t, func() {
			listener.addMessage(new(Message).Init(c.message, "test"))

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
	producer, _ := new(Producer).InitC("your-stream", "0", ShardIterTypes[3], "accesskey", "secretkey", "us-east-1", 10)

	listener.NewEndpoint(testEndpoint, "your-stream")
	producer.NewEndpoint(testEndpoint, "your-stream")

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
