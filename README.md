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
import "github.com/rewardStyle/kinetic"

// Use configuration in /etc/kinetic.conf
listener, _ := new(kinetic.Listener).Init()

// Use custom configuration
producer, _ := new(kinetic.Producer).InitWithConf("your-stream", "0", "shard-type", "accesskey", "secretkey", "region")

producer.Send(new(kinetic.Message).Init([]byte(`{"foo":"bar"}`), "test"))

// Using Retrieve
msg, err := listener.Retrieve()
if err != nil {
    println(err)
}

println(string(msg))

// Using Listen - will block unless sent in goroutine
go listener.Listen(func(msg []byte, wg *sync.WaitGroup) {
    println(string(msg))
    wg.Done()
})

producer.Send(new(KinesisMessage).Init([]byte(`{"foo":"bar"}`), "test"))

listener.Close()
producer.Close()

// Or with Kinesis Firehose
firehose, err := new(kinetic.Producer).Firehose()
if err != nil {
    println(err)
}

// Will add a newline character to each message
firehose.Send(new(KinesisMessage).Init([]byte(`{"foo":"bar"}`), "test"))

firehose.Close()

```

For more examples take a look at the tests. API documentation can be found [here](https://godoc.org/github.com/rewardStyle/kinetic).
