# kinetic
Kinetic is a High-performance AWS Kinesis Client for Go

Kinetic wraps [sendgridlabs go-kinesis library](https://github.com/sendgridlabs/go-kinesis) to provide maximum throughput for AWS Kinesis producers and consumers.
An instance of a Kinetic listener/producer is meant to be used for each shard, so please use it accordingly. If you use more than one instance per-shard then you will
hit the AWS Kinesis throughput [limits](http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html).

### Getting Started
Before using kinetic, you should make sure you have a created a Kinesis stream and your configuration file has the credentails necessary to read and write to the stream. Once this stream exists in AWS, kinetic will ensure it is in the "ACTIVE" state before running.


## Testing
Tests are written using [goconvey](http://goconvey.co/). They can be run either via the comamnd line:

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
listener, _ := new(Listener).Init("kinesis.us-east-1.amazonaws.com/stream", 0)
producer, _ := new(Producer).Init("kinesis.us-east-1.amazonaws.com/stream", 0)

producer.Send(new(KinesisMessage).Init([]byte(`{"foo":"bar"}`), "test"))

// Using Retrieve
msg, err := listener.Retrieve()
if err != nil {
    t.Fatalf(err.Error())
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
```

For more examples take a look at the tests.
