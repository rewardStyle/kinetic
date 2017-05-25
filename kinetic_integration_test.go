package kinetic

import (
	"encoding/json"
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/rewardStyle/kinetic/listener"
	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/producer"
	"github.com/stretchr/testify/assert"
)

type Message struct {
	Id      int    `json:"id"`
	Message string `json:"message"`
}

type StreamData struct {
	mutex       sync.Mutex
	Frequencies map[int]int
	Messages    map[int][]string
}

func NewStreamData() *StreamData {
	return &StreamData{
		mutex: sync.Mutex{},
		Frequencies: make(map[int]int),
		Messages: make(map[int][]string),
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

	// Instantiate a new kinentic object
	k, err := New(func(c *Config) {
		c.SetCredentials("some-access-key", "some-secret-key", "some-security-token")
		c.SetRegion("some-region")
		c.SetEndpoint("http://127.0.0.1:4567")
	})
	assert.NotNil(t, k)
	assert.Nil(t, err)

	// Create a kinetic stream
	stream := "some-stream-" + strconv.Itoa(rand.Int())
	err = k.CreateStream(stream, 1)
	assert.Nil(t, err)

	// Delete the kinetic stream if no dups were found (this is for debugging the kinetic stream)
	duplicate := false
	defer func() {
		if !duplicate {
			k.DeleteStream(stream)
			k.WaitUntilStreamDeleted(context.TODO(), stream,
				request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		}
	}()

	// Wait until the stream is ready to go
	err = k.WaitUntilStreamExists(context.TODO(), stream,
		request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
	assert.Nil(t, err)

	// Determine the shard name
	shards, err := k.GetShards(stream)
	assert.Equal(t, len(shards), 1)
	assert.Nil(t, err)

	log.Printf("Stream Name: %s\n", stream)
	log.Printf("Shard Name: %s\n", shards[0])

	// Create a new kinetic producer
	p, err := producer.NewProducer(func(c *producer.Config) {
		c.SetAwsConfig(k.Session.Config)
		c.SetKinesisStream(stream)
		c.SetBatchSize(5)
		c.SetBatchTimeout(1000 * time.Millisecond)
	})
	assert.NotNil(t, p)
	assert.Nil(t, err)

	// Create a new kinetic listener
	l, err := listener.NewListener(func(c *listener.Config) {
		c.SetAwsConfig(k.Session.Config)
		c.SetReader(listener.NewKinesisReader(stream, shards[0]))
		c.SetQueueDepth(20)
		c.SetConcurrency(10)
		c.SetGetRecordsReadTimeout(1000 * time.Millisecond)
		//c.SetLogLevel(aws.LogDebug)
	})
	assert.NotNil(t, l)
	assert.Nil(t, err)

	numMsg := 1000
	numSent := 0
	streamData := NewStreamData()

	// Use the producer to write messages to the kinetic stream
	wg := sync.WaitGroup{}
	wg.Add(numMsg + 1)
	go func(sent *int) {
		for i := 0; i < numMsg; i++ {
			msg := &Message{
				Id: i,
				Message: "hello_" + strconv.Itoa(i),
			}
			jsonStr, _ := json.Marshal(msg)
			if err := p.Send(&message.Message {
				PartitionKey: aws.String("key"),
				Data: []byte(jsonStr),
			}); err == nil {
				*sent++
			}
		}
		wg.Done()
	}(&numSent)

	// Use the listener to read messages from the kinetic stream
	go func() {
		l.Listen(func(b []byte, fnwg *sync.WaitGroup) {
			msg := &Message{}
			json.Unmarshal(b, msg)

			if !streamData.exists(msg.Id) {
				wg.Done()
			} else {
				log.Printf("WARNING: Duplicate message: %v\n", msg)
			}

			streamData.put(msg.Id, msg.Message)
			fnwg.Done()
		})
	}()

	wg.Wait()
	assert.Equal(t, int(numSent), numMsg, "Number of message sent should equal the number of messages")
	assert.Equal(t, streamData.size(), numMsg, "Number of messages")

	streamData.printSummary()
	duplicate = streamData.hasDuplicates()
}
