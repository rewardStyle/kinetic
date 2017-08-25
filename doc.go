/*
Package kinetic is a library written in Go that provides a scalable, high-performance and
fault-tolerant means to write to and read from an Amazon Kinesis stream.

The kinetic library is distributed under an MIT License (see LICENSE.txt file).

The kinetic library wraps the aws-sdk-go library (http://github.com/aws/aws-sdk-go.git) to provide
maximum throughput streaming to/from an Amazon Kinesis stream with built-in fault tolerance and
retry logic via the kinetic Producer and Consumer.  The kinetic Producer can write to Kinesis
via the KinesisWriter or to Firehose via the FirehoseWriter.  The kinetic Consumer can stream
data from Kinesis using the aws-sdk-go library via the KinesisReader or by using the Kinesis
Client Library
(http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html) via the
KclReader.

Kinetic

The Kinetic object provides access to a Kinesis client via the kinesisiface API
(https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/kinesisiface/) in
addition to providing utility methods for working with Kinesis streams such as creating and
deleting streams and retrieving shard information.  When instantiating a Kinetic object, a new
AWS session is created using your AWS configuration (credentials, region and endpoint).  Kinetic
also supports streaming to/from a local kinesalite (https://hub.docker.com/r/dlsniper/kinesalite/)
instance.

A new Kinetic object can be instantiate using the NewKinetic() function as such:

	k, err := kinetic.NewKinetic(
		kinetic.AwsConfigCredentials("some-access-key", "some-secret-key", "some-security-token"),
		kinetic.AwsConfigRegion("some-region"),
		kinetic.AwsConfigEndpoint("http://127.0.0.1:4567"),
	)

which accepts a variable number of functional option methods.

Producer

The Producer object is used to stream data to an Amazon Kinesis or Firehose stream in batches via
the Send, SendWithContext or TryToSend functions.

The Producer achieves optimal throughput by implementing a dispatcher/worker model to pull
messages off of a queue and send batches concurrently (within rate limits) based on the number
active shards, which is automatically adjusted after a re-sharding operation occurs externally.

Usage:

To create a Producer with default values, call NewProducer() with a pointer to the
aws.Config struct and the stream name as such:

	p, err := kinetic.NewProducer(k.Session.Config, "some-stream-name")

where k is the Kinetic object created (above).

To instantiate a Producer with custom parameters, pass in functional option methods like this:

	psc := kinetic.NewDefaultProducerStatsCollector(registry)
	w, err := kinetic.NewKinesisWriter(k.Session.Config, "some-stream-name",
		kinetic.KinesisWriterResponseReadTimeout(time.Second),
		kinetic.KinesisWriterMsgCountRateLimit(1000),
		kinetic.KinesisWriterMsgSizeRateLimit(1000000),
		kinetic.KinesisWriterLogLevel(aws.LogOff),
		kinetic.KinesisWriterStats(psc),
	)

	p, err := kinetic.NewProducer(k.Session.Config, "some-stream-name",
		kinetic.ProducerWriter(w),
		kinetic.ProducerBatchSize(500),
		kinetic.ProducerBatchTimeout(time.Second),
		kinetic.ProducerMaxRetryAttempts(3),
		kinetic.ProducerQueueDepth(10000),
		kinetic.ProducerConcurrency(3),
		kinetic.ProducerShardCheckFrequency(time.Minute),
		kinetic.ProducerDataSpillFn(func(msg *kinetic.Message) error {
			log.Printf("Message was dropped: [%s]\n", string(msg.Data))
			return nil
		}),
		kinetic.ProducerLogLevel(aws.LogOff),
		kinetic.ProducerStats(psc),
	)

Then to send messages:

	jsonStr, err := json.Marshal(yourDataStruct)
	msg := &kinetic.Message{
		PartitionKey: aws.String("unique-id-per-message"),
		Data:         []byte(jsonStr),
	}

	// Drops message to data spill function if message channel is full
	p.TryToSend(msg)

OR

	// Blocks until message gets enqueued onto the message channel
	p.Send(msg)

How it works:

The Producer is started by calling one of the Send APIs (Send / SendWithContext / TryToSend) which
kicks off two goroutines: 1) one to periodically check the number of active shards and adjust the
number of concurrent workers accordingly and 2) one to dispatch messages to the workers and
throttle the number of messages per second and the transmission (memory) size of the batch per
second based on the stream limits set by AWS
(http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html).  Messages sent
via the Send APIs are enqueued in a message channel which get batched and sent via the
dispatcher/worker model.

If the shard monitoring determines that there are too few workers available based on the number of
active shards, it will spawn additional workers which run in separate goroutines.  If the shard
monitoring determines that there are too many workers available based on the number of active
shards, it will send a signal to a dismiss channel to which workers actively listen.  The rate
limiters get reset after a change is detected.

After being spawned, a worker runs indefinitely doing two things: 1) communicating to the
dispatcher (via a status channel) how many messages it can send in its next batch (based on the
batch size less the number of previously failed messages) and 2) calling the Producer's sendBatch
function AFTER receiving a new batch from the dispatcher.  If any messages failed to send in the
sendBatch call, those messages are retried by the worker in subsequent batches until they reach
the maximum number of retry attempts, after which they are sent to a data spill callback function.
Workers run indefinitely until they receive a signal to dismiss AND they have no retry messages
left to send.  The status channel is an unbuffered channel, so workers will idle while trying to
communicate to the dispatcher on this channel when the message volume is low.

The dispatcher does the following: 1) listens to the status channel for workers signaling their
availability and capacity 2) pulls messages off of the message channel and 3) throttles the
outgoing messages using a token bucket based on the allowed rate limits.  Two concurrent
goroutinues are started in parallel (one for the message count of the batch and one for the
memory transmission size of the batch) to wait until the respective tokens are available.

In a high volume scenario, the rate limiters prevent the Producer from sending too many messages
in a short period of time which would cause provisioned throughput exceeded exceptions.
In a low volume scenario, a batch timeout causes batches to be sent with fewer messages than the
maximum batch limit so the rate limiters do not throttle the outgoing messages.

KinesisWriter

The KinesisWriter is the default writer used by the Producer to stream data.  To stream
messages to Amazon Firehose use the FirehoseWriter.

The KinesisWriter implements the StreamWriter interface using the kinesisiface API to make
PutRecordsRequests.  Successfully sent messages are updated with meta data containing the
sequence number (http://docs.aws.amazon.com/streams/latest/dev/key-concepts.html#sequence-number)
and the shardId. Failed messages are updated with meta data which tracks the error code, error
message and fail count.

To create a custom Producer with a KinesisWriter, see the example in Producer (above).

FirehoseWriter

The FirehoseWriter implements the StreamWriter interface using the firehoseiface API to make
PutRecordBatchRequests.  Successfully sent messages are updated with meta data containing the
sequence number (http://docs.aws.amazon.com/streams/latest/dev/key-concepts.html#sequence-number)
and the shardId. Failed messages are updated with meta data which tracks the error code, error
message and fail count.

To create a Producer with a FirehoseWriter, use the functional option methods like this:

	psc := kinetic.NewDefaultProducerStatsCollector(registry)
	w, err := kinetic.NewFirehoseWriter(k.Session.Config, "some-stream-name",
		kinetic.FirehoseWriterResponseReadTimeout(time.Second),
		kinetic.FirehoseWriterMsgCountRateLimit(1000),
		kinetic.FirehoseWriterMsgSizeRateLimit(1000000),
		kinetic.FirehoseWriterLogLevel(aws.LogOff),
		kinetic.FirehoseWriterStats(psc),
	)

	p, err := kinetic.NewProducer(k.Session.Config, "some-stream-name",
		kinetic.ProducerWriter(w),
		kinetic.ProducerBatchSize(500),
		kinetic.ProducerBatchTimeout(time.Second),
		kinetic.ProducerMaxRetryAttempts(3),
		kinetic.ProducerQueueDepth(10000),
		kinetic.ProducerConcurrency(3),
		kinetic.ProducerShardCheckFrequency(time.Minute),
		kinetic.ProducerDataSpillFn(func(msg *kinetic.Message) error {
			log.Printf("Message was dropped: [%s]\n", string(msg.Data))
			return nil
		}),
		kinetic.ProducerLogLevel(aws.LogOff),
		kinetic.ProducerStats(psc),
	)

Consumer

The Consumer object is used to stream data from an Amazon Kinesis stream in batches via the
Retrieve or Listen functions.

The Consumer achieves optimal throughput by implementing a dispatcher/worker model to pull
messages off of a queue and send batches concurrently (within rate limits) based on the number
active shards, which is automatically adjusted after a re-sharding operation occurs externally.

Usage:

To create a Consumer with default values, call the NewConsumer function like this:

	c, err := kinetic.NewConsumer(k.Session.Config, "some-stream-name", "some-shard-name")

where k is the Kinetic object created (above)

To create a Consumer with custom parameters, pass in functional option methods like this:

	csc := kinetic.NewDefaultConsumerStatsCollector(registry)
	r, err := kinetic.NewKinesisReader(k.Session.Config, "some-stream-name", "some-shard-id",
		kinetic.KinesisReaderBatchSize(10000),
		kinetic.KinesisReaderResponseReadTimeout(time.Second),
		kinetic.KinesisReaderLogLevel(aws.LogOff),
		kinetic.KinesisReaderStats(csc),
	)

	c, err := kinetic.NewConsumer(k.Session.Config, "some-stream-name", "some-shard-id",
		kinetic.ConsumerReader(r),
		kinetic.ConsumerQueueDepth(500),
		kinetic.ConsumerConcurrency(10),
		kinetic.ConsumerLogLevel(aws.LogOff),
		kinetic.ConsumerStats(csc),
	)

Then to retrieve messages:

	// To retrieve a single message off of the message queue
	message, err := c.Retrieve()
	msg := &YourDataStruct{}
	json.Unmarshal(message.Data, msg)

	// Do something with msg

OR

	// To listen to the message channel indefinitely
	c.Listen(func(m *kinetic.Message) error {
		// Unmarshal data
		msg := &YourDataStruct{}
		err := json.Unmarshal(m.Data, msg)

		// Do something with msg

		return err
	})

How it works:

The Consumer is started by calling one of the Retrieve / Listen APIs which kicks off a goroutine
that does two things in an infinite loop: 1) invokes a GetRecords call and 2) enqueues the batch
of messages from the GetRecords call to a message queue.  The GetRecords calls are throttled by
a rate limiter which utilizes a token bucket system for the number of GetRecords transactions per
second and the transmission (memory) size of the batch per second.  Because the message queue is
a buffered message channel, this goroutine becomes blocked once the message queue is full.

The Retrieve APIs (Retrieve, RetrieveWithContext, RetrieveFn and RetrieveFnWithContext) pull one
message off of the message queue while the Listen APIs (Listen and ListenWithContext) pull
messages off of the message queue concurrently based on the Consumer's concurrency setting.  The
supplied callback function for the Listen APIs is run (asynchronously) on the messages as the are
pulled off.

KinesisReader

The KinesisReader is the default reader used by the Consumer to stream data.

The KinesisReader implements the StreamReader interface using the kinesisiface API to make
GetRecordsRequest, the results of which are enqueued to the Consumer's message queue.

To create a custom Consumer with a KinesisReader, see the example for Consumer (above).

KclReader

The KclReader implements the StreamReader interface and adheres to the MultiLangDaemon protocol
to communicate with the Kinetic Client Libray (KCL) over STDIN and STDOUT as documented here:
https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonawservices/kinesis/multilang/package-info.java

The KCL is a Java library which requires a Java Runtime Environment to run
(http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html).  If you
choose to use the KclReader to stream your data from Amazon Kinesis (instead of the
KinesisReader), your application must have a JRE and must run the MultiLangDaemon as a background
process.

To create a custom Consumer with a KclReader, use the functional option methods as such:

	csc := kinetic.NewDefaultConsumerStatsCollector(registry)
	r, err := kinetic.NewKclReader(k.Session.Config, "some-stream-name", "some-shard-id",
		kinetic.KclReaderAutoCheckpointCount(1000),
		kinetic.KclReaderAutoCheckpointFreq(time.Minute),
		kinetic.KclReaderUpdateCheckpointSizeFreq(time.Minute),
		kinetic.KclReaderOnInitCallbackFn(func () {return nil}),
		kinetic.KclReaderOnCheckpointCallbackFn(func () {return nil}),
		kinetic.KclReaderOnShutdownCallbackFn(func () {return nil}),
		kinetic.KclReaderLogLevel(aws.LogOff),
		kinetic.KclReaderStats(csc),
	)

	c, err := kinetic.NewConsumer(k.Session.Config, "some-stream-name", "some-shard-id",
		kinetic.ConsumerReader(r),
		kinetic.ConsumerQueueDepth(500),
		kinetic.ConsumerConcurrency(10),
		kinetic.ConsumerLogLevel(aws.LogOff),
		kinetic.ConsumerStats(csc),
	)

How it works:

The KclReader is started by calling one of the Retrieve / Listen APIs of the Consumer which kicks
off separate goroutines that write to STDOUT and read from STDIN in accordance with the
MultiLangDaemon protocol.

	The multi language protocol defines a system for communication  between a KCL multi-lang
	application and another process (referred to as the "child process") over STDIN and
	STDOUT of the child process.

The sequence of events that occurs between the KCL process and the KclReader goes something like
this:

	* an "initialize" action message is sent to the child process which the KclReader reads
	from STDIN.
	* KclReader sends an acknowledgement status message to STDOUT.
	* a "processRecord" message is sent to the child process which the KclReader reads from
	STDIN.
		- In a separate goroutine, the KclReader ranges through each record enqueueing
		the message to an internal message channel (unbuffered)
		- messages are pulled off the queue as needed by the Consumer's GetRecord and
		GetRecords APIs
		- upon completion of the batch of messages sent by KCL...
	* KclReader sends an acknowledgement stats message to STDOUT.
	* At any point after the KclReader sends acknowledgement status message for the
	"initialize" status message, the KclReader can send a "checkpoint" status message to KCL
	over STDOUT.
	* KCL then sends an acknowledgement to the "checkpoint" status message
	* When KCL sends a "shutdown" action message, KclReader invokes an OnShutdown callback
	function and sends an acknowledgement to KCL over STDOUT.

It is important to note that checkpointing can occur in between receiving a "processRecord"
message from KCL and sending an acknowledgement message.

Checkpointing:

Checkpointing is a feature that is only available for the KclReader.  Checkpointing allows your
application to indicate to the KclReader that all records up to and including a given sequence
number have been successfully received and processed by your application so that if your
application were to be restarted (for whatever reason) the streaming would start with the next
sequence number instead of the TRIM_HORIZON.  Checkpointing is not necessary if your application
consuming Kinesis data is idempotent, but starting from the TRIM_HORIZON on restarts will create
unnecessary (and avoidable) processing.

The KclReader provides three methods for checkpointing:  (CheckpointInsert, CheckpointDelete and
Checkpoint).

Usage of the Checkpoint APIs goes something like this:

	r, _ := kinetic.NewKclReader(
		kinetic.KclReaderAutoCheckpointCount(1000),
		kinetic.KclReaderAutoCheckpointFreq(time.Hour),
		kinetic.KclReaderOnCheckpointCallbackFn(
			func (seqNum string, err error) error {
				return nil
			},
		),
		// Additional functional option methods here
	)

	c, err := kinetic.NewConsumer(k.Session.Config, "some-stream-name", "some-shard-id",
		kinetic.ConsumerReader(r),
		// Additional functional option methods here
	)

	// Stream data from Kinesis
	go func() {
		c.Listen(func (m *kinetic.Message) error {
			r.CheckpointInsert(m.SequenceNumber)
			defer r.CheckpointDone(m.SequenceNumber)

			// Unmarshal data
			msg := &Message{}
			json.Unmarshal(m.Data, msg)

			// Do stuff with your data
		})
	}()

	// Periodically call checkpoint
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer r.ticker.Stop()

		select {
		case <-ticker.C:
			r.Checkpoint()
		case <-stop:
			return
		}
	}()
*/
package kinetic