package kinetic

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/assert"
)

type TestMessage struct {
	ID      int    `json:"id"`
	Message string `json:"message"`
}

type StreamData struct {
	mutex       sync.Mutex
	Frequencies map[int]int
	Messages    map[int][]string
}

func NewStreamData() *StreamData {
	return &StreamData{
		mutex:       sync.Mutex{},
		Frequencies: make(map[int]int),
		Messages:    make(map[int][]string),
	}
}

func (m *StreamData) put(key int, value string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Frequencies[key] = m.Frequencies[key] + 1
	m.Messages[key] = append(m.Messages[key], value)
}

func (m *StreamData) exists(key int) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.Frequencies[key] > 0
}

func (m *StreamData) size() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.Messages)
}

func (m *StreamData) hasDuplicates() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, freq := range m.Frequencies {
		if freq > 1 {
			return true
		}
	}
	return false
}

func (m *StreamData) printSummary() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	log.Println("***** Stream Data Summary *****")
	log.Printf("Total messages sent/received: [%d]\n", len(m.Messages))
	for index, freq := range m.Frequencies {
		if freq > 1 {
			log.Printf("Message [%d] occurred [%d] times\n", index, freq)
		}
	}
	log.Println("***** Stream Data Summary *****")
}

func TestKineticIntegration(t *testing.T) {

	// Set the RNG Seed based on current time (in order to randomize the RNG)
	rand.Seed(time.Now().UTC().UnixNano())

	// Instantiate StreamData Object to keep stats
	streamData := NewStreamData()

	// Instantiate a new kinentic object
	k, err := NewKinetic(
		KineticAwsConfigCredentials("some-access-key", "some-secret-key", "some-security-token"),
		KineticAwsConfigRegion("some-region"),
		KineticAwsConfigEndpoint("http://127.0.0.1:4567"),
	)
	assert.NotNil(t, k)
	assert.Nil(t, err)

	// Create a kinetic stream
	stream := "some-stream-" + strconv.Itoa(rand.Int())
	err = k.CreateStream(stream, 1)
	assert.Nil(t, err)

	// Wait until the stream is ready to go
	err = k.WaitUntilStreamExists(context.TODO(), stream,
		request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
	assert.Nil(t, err)

	// Delete the kinetic stream if no dups were found (this is for debugging the kinetic stream)
	defer func(s *StreamData) {
		if !s.hasDuplicates() {
			k.DeleteStream(stream)
			k.WaitUntilStreamDeleted(context.TODO(), stream,
				request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		}
	}(streamData)

	// Determine the shard name
	shards, err := k.GetShards(stream)
	assert.Equal(t, len(shards), 1)
	assert.Nil(t, err)

	log.Printf("Stream Name: %s\n", stream)
	log.Printf("Shard Name: %s\n", shards[0])

	// Create a new kinesis stream writer
	w, err := NewKinesisWriter(k.Session.Config, stream,
		KinesisWriterResponseReadTimeout(time.Second),
		KinesisWriterMsgCountRateLimit(1000),
		KinesisWriterMsgSizeRateLimit(1000000),
		KinesisWriterLogLevel(LogDebug),
	)
	if err != nil {
		log.Fatalf("Unable to create a new kinesis stream writer due to: %v\n", err)
	}

	// Create a new kinetic producer
	p, err := NewProducer(k.Session.Config, w,
		ProducerBatchSize(5),
		ProducerBatchTimeout(time.Second),
		ProducerMaxRetryAttempts(3),
		ProducerQueueDepth(10000),
		ProducerConcurrency(3),
		ProducerShardCheckFrequency(time.Minute),
		ProducerDataSpillFn(func(msg *Message) error {
			//log.Printf("Message was dropped: [%s]\n", string(msg.Data))
			return nil
		}),
		ProducerLogLevel(aws.LogOff),
		ProducerStats(&NilProducerStatsCollector{}),
	)
	assert.NotNil(t, p)
	assert.Nil(t, err)

	assert.NotNil(t, k.Session)
	assert.NotNil(t, k.Session.Config)
	r, err := NewKinesisReader(k.Session.Config, stream, shards[0],
		//KinesisReaderBatchSize(),
		//KinesisReaderShardIterator(),
		KinesisReaderResponseReadTimeout(time.Second),
		//KinesisReaderLogLevel(),
		//KinesisReaderStatsCollector(),
	)
	assert.NotNil(t, r)
	assert.NoError(t, err)

	// Create a new kinetic listener
	l, err := NewConsumer(k.Session.Config, r,
		ConsumerQueueDepth(20),
		ConsumerConcurrency(10),
		ConsumerLogLevel(aws.LogOff),
		ConsumerStats(&NilConsumerStatsCollector{}),
	)
	assert.NotNil(t, l)
	assert.Nil(t, err)

	numMsg := 1000
	numSent := 0

	// Use the producer to write messages to the kinetic stream
	wg := sync.WaitGroup{}
	wg.Add(numMsg + 1)
	go func(sent *int) {
		defer wg.Done()
		for i := 0; i < numMsg; i++ {
			msg := &TestMessage{
				ID:      i,
				Message: "hello_" + strconv.Itoa(i),
			}
			jsonStr, _ := json.Marshal(msg)
			if err := p.Send(&Message{
				PartitionKey: aws.String("key"),
				Data:         []byte(jsonStr),
			}); err == nil {
				*sent++
			}
		}
	}(&numSent)

	// Use the listener to read messages from the kinetic stream
	go func() {
		l.Listen(func(m *Message, fnwg *sync.WaitGroup) error {
			defer fnwg.Done()

			msg := &TestMessage{}
			json.Unmarshal(m.Data, msg)

			if !streamData.exists(msg.ID) {
				wg.Done()
			} else {
				log.Printf("WARNING: Duplicate message: %v\n", msg)
			}

			streamData.put(msg.ID, msg.Message)

			return nil
		})
	}()

	wg.Wait()
	assert.Equal(t, int(numSent), numMsg, "Number of message sent should equal the number of messages")
	assert.Equal(t, streamData.size(), numMsg, "Number of messages")

	streamData.printSummary()
}
