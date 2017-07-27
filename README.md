[![GoDoc](https://godoc.org/github.com/rewardStyle/kinetic?status.svg)](https://godoc.org/github.com/rewardStyle/kinetic)
[![Circle CI](https://circleci.com/gh/rewardStyle/kinetic/tree/master.svg?style=svg&circle-token=8c8b6e0cca0f0fde6ec41b4e02329c406f74a446)](https://circleci.com/gh/rewardStyle/kinetic/tree/master)

# kinetic
Kinetic is an MIT-licensed high-performance AWS Kinesis Client for Go

Kinetic wraps [sendgridlabs go-kinesis library](https://github.com/sendgridlabs/go-kinesis) to provide maximum throughput for AWS Kinesis producers and consumers.
An instance of a Kinetic listener/producer is meant to be used for each shard, so please use it accordingly. If you use more than one instance per-shard then you will
hit the AWS Kinesis throughput [limits](http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html).

### Getting Started
Before using kinetic, you should make sure you have a created a Kinesis stream and your configuration file has the credentails necessary to read and write to the stream. Once this stream exists in AWS, kinetic will ensure it is in the "ACTIVE" state before running.


## Testing
Tests are written using [goconvey](http://goconvey.co/) and [kinesalite](https://github.com/mhart/kinesalite). Make sure you have kinesalite running locally before attempting to run the tests. They can be run either via the comamnd line:


```sh
$ go test -v -cover -race
```

or via web interface:

```sh
$ goconvey
```

## Running
Kinetic can be used to interface with kinesis like so:


```go
import (
	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/listener"
	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/producer"
	"sync"
)

// Create a kinetic object associated with a local kinesalite stream
k, _ := kinetic.New(func(c *kinetic.Config) {
    c.SetCredentials("some-access-key", "some-secret-key", "some-security-token")
    c.SetRegion("some-region")
    c.SetEndpoint("http://127.0.0.1:4567")
})

// Create a kinetic producer
p, _ := producer.NewProducer(func(c *producer.Config) {
    c.SetAwsConfig(k.Session.Config)
    c.SetKinesisStream("stream-name")
})

// Create a kinetic listener
l, _ := listener.NewListener(func(c *listener.Config) {
    c.SetAwsConfig(k.Session.Config)
    c.SetReader(listener.NewKinesisReader("stream-name", "shard-name"))
})

msg, err := l.Retrieve()
if err != nil {
    println(err)
}

// Using Listen - will block unless sent in goroutine
go l.Listen(func(b []byte, fnwg *sync.WaitGroup){
    println(string(b))
    fnwg.Done()
})

// Send a message using the producer 
p.Send(&kinetic.Message{
    Data: []byte(`{"foo":"bar"}`),
})

```

For more examples take a look at the tests or the test program in the `testexec` directory.  API documentation can be found [here](https://godoc.org/github.com/rewardStyle/kinetic).
