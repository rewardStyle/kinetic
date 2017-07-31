[![GoDoc](https://godoc.org/github.com/rewardStyle/kinetic?status.svg)](https://godoc.org/github.com/rewardStyle/kinetic)
[![Circle CI](https://circleci.com/gh/rewardStyle/kinetic/tree/master.svg?style=svg&circle-token=8c8b6e0cca0f0fde6ec41b4e02329c406f74a446)](https://circleci.com/gh/rewardStyle/kinetic/tree/master)

# kinetic
Kinetic is an MIT-licensed high-performance AWS Kinesis Client for Go

Kinetic wraps [aws-sdk-go](https://github.com/aws/aws-sdk-go.git) to provide maximum throughput with built-in fault tolerance and retry logic for AWS Kinesis producers and consumers.
The Kinetic producer can write to Kinesis or Firehose and the Kinetic listener can consume stream data from Kinesis using the aws-go-sdk or using the Kinesis client library (written in Java).  

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
	"sync"
)

// Create a kinetic object associated with a local kinesalite stream
k, _ := kinetic.NewKinetic(
    kinetic.AwsConfigCredentials("some-access-key", "some-secret-key", "some-security-token"),
    kinetic.AwsConfigRegion("some-region"),
    kinetic.AwsConfigEndpoint(""http://127.0.0.1:4567""),
)

// Create a kinetic producer
p, _ := kinetic.NewProducer(k.Session.Config, "stream-name")

// Create a kinetic consumer
c, err := kinetic.NewConsumer(k.Session.Config, "stream-name", "shard-name")


// Retrieve one message using the consumer's Retrieve function
msg, err := c.Retrieve()
if err != nil {
    println(err)
}

// Using Listen - will block unless sent in goroutine
go c.Listen(func(b []byte, wg *sync.WaitGroup){
    defer wg.Done()
    
    println(string(b))
})

// Send a message using the producer 
p.Send(&kinetic.Message{
    Data: []byte(`{"foo":"bar"}`),
})

```

For more examples take a look at the tests or the test program in the `testexec` directory.  API documentation can be found [here](https://godoc.org/github.com/rewardStyle/kinetic).
