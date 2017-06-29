package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/listener"
	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/producer"
)

// Declare global variables
var startTime time.Time
var streamStart time.Time
var streamStop time.Time
var pipeOfDeath chan os.Signal
var stopDisplay chan struct{}
var stopProducing chan struct{}
var config *Config

func init() {
	// Start the stopwatch
	startTime = time.Now()

	// Set the RNG Seed based on current time (in order to randomize the RNG)
	rand.Seed(startTime.UTC().UnixNano())

	// Instantiate channels for communicating between threads
	pipeOfDeath = make(chan os.Signal, 1)
	stopDisplay = make(chan struct{}, 1)
	stopProducing = make(chan struct{}, 1)

	// Set up pipeOfDeath channel to receive os signals
	signal.Notify(pipeOfDeath, os.Interrupt)
}

func cleanup(k *kinetic.Kinetic, streamName string) {
	if *config.Cleanup {
		if *config.Verbose {
			log.Println()
			log.Printf("Cleaning up by deleting stream [%s] ...\n", streamName)
		}
		k.DeleteStream(streamName)
		if *config.Verbose {
			log.Printf("Waiting for stream [%s] to be deleted ...\n", streamName)
		}
		k.WaitUntilStreamDeleted(context.TODO(), streamName,
			request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		if *config.Verbose {
			log.Println("Clean up complete")
			log.Println()
		}
	}
}

func main() {
	// Process command line arguments
	config = parseCommandLineArgs()
	config.printConfigs()

	// Instantiate a new kinetic object based on the location of the kinesis stream
	var k *kinetic.Kinetic
	switch strings.ToLower(*config.Location) {
	case "local":
		k = newDefaultKinetic()
	case "aws":
		k = newAwsKinetic()
	default :
		log.Fatalf("Unknown location for kinesis stream: %s\n", *config.Location)
	}

	// Set streamName from config or generate a random one
	streamName := *config.StreamName
	if streamName == "" {
		streamName = generateRandomStreamName()
	}

	// Create and wait for a new kinetic stream (if the stream name doesn't already exist)
	prepareKinesisStream(k, streamName)

	// Create a new kinetic producer
	p := newKineticProducer(k, streamName)

	// Create a new kinetic listener
	l := newKineticListener(k, streamName)

	// Instantiate StreamData Object to keep stats
	streamData := NewStreamData()

	// Display stream data statistics to the console
	displayWg := sync.WaitGroup{}
	displayWg.Add(1)
	go func(sd *StreamData) {
		displayLoop:
			for {
				select {
				case <-pipeOfDeath:
					stopProducing <- struct{}{}
					if *config.Verbose {
						log.Println()
						log.Println("display: Received pipeOfDeath ...")
					}
				case <-stopDisplay:
					if *config.Verbose {
						log.Println()
						log.Print("display: Received stopDisplay ...")
					}
					break displayLoop
				default:
					time.Sleep(1000 * time.Millisecond)
					log.Println()
					log.Printf("Stream name: %s\n", streamName)
					log.Printf("Elapsed Time: %v\n", time.Since(startTime))
					if streamStop.IsZero() {
						log.Printf("Streaming Time: %v\n", time.Since(streamStart))
					} else {
						log.Printf("Streaming Time: %v\n", streamStop.Sub(streamStart))
					}
					sd.printStats()
				}
			}
		streamData.printSummary()
		displayWg.Done()
	}(streamData)

	// Use the producer to write messages to the kinetic stream
	streamWg := sync.WaitGroup{}
	streamWg.Add(1)
	go func(sd *StreamData) {
		streamStart = time.Now()
		if config.Duration != nil {
			// Set a timeout based on the config
			var timeout <-chan time.Time
			if *config.Duration > 0 {
				timeout = time.After(time.Duration(*config.Duration) * time.Second)
			} else {
				timeout = make(chan time.Time, 1)
			}

			produceLoop:
				for {
					select {
					case <-stopProducing:
						if *config.Verbose {
							log.Println()
							log.Print("producer: Received stopProducing ...")
						}
						streamStop = time.Now()
						break produceLoop
					case <-timeout:
						if *config.Verbose {
							log.Println()
							log.Print("producer: Timed out ...")
						}
						streamStop = time.Now()
						break produceLoop
					default:
						jsonStr, _ := json.Marshal(NewMessage())
						if err := p.Send(&message.Message {
							PartitionKey: aws.String("key"),
							Data: []byte(jsonStr),
						}); err == nil {
							streamWg.Add(1)
							sd.incrementMsgCount()
						}
					}
				}
		} else if config.NumMsgs != nil {
			for i := 0; i < *config.NumMsgs; i++ {
				jsonStr, _ := json.Marshal(NewMessage())
				if err := p.Send(&message.Message {
					PartitionKey: aws.String("key"),
					Data: []byte(jsonStr),
				}); err == nil {
					streamWg.Add(1)
					sd.incrementMsgCount()
				}
			}
			streamStop = time.Now()
		}
		streamWg.Done()
	}(streamData)

	// Use the listener to read messages from the kinetic stream
	go func(sd *StreamData) {
		l.Listen(func(m *message.Message, fnwg *sync.WaitGroup) error {
			// Unmarshal data
			msg := &Message{}
			json.Unmarshal(m.Data, msg)

			// Only mark "done" if the message isn't a duplicate
			if !sd.exists(msg.ID) {
				streamWg.Done()
			} else {
				if *config.Verbose {
					log.Printf("listner: Duplicate message: %v\n", msg)
				}
			}

			// Record message regardless if it is a duplicate
			sd.mark(msg.ID)
			fnwg.Done()

			return nil
		})
	}(streamData)

	// Wait until streaming is complete
	streamWg.Wait()
	stopDisplay <- struct{}{}

	// Wait until output display is complete
	displayWg.Wait()
	cleanup(k, streamName)
}

func newDefaultKinetic() *kinetic.Kinetic {
	k, err := kinetic.New(func(c *kinetic.Config) {
		c.SetCredentials("some-access-key", "some-secret-key", "some-security-token")
		c.SetRegion("some-region")
		c.SetEndpoint("http://127.0.0.1:4567")
	})
	if err != nil {
		log.Fatalf("Unable to create new default kinetic object due to: %v\n", err)
	}

	return k
}

func newAwsKinetic() *kinetic.Kinetic {
	// Verify that ~/.aws/credentials file exists
	// TODO: Fix this bug
	//if _, err := os.Stat("~/.aws/credentials"); os.IsNotExist(err) {
	//	log.Fatal("~/.aws/credentials does not exist")
	//}

	// Verify that ~/.aws/config file exists
	// TODO: Fix this bug
	//if _, err := os.Stat("~/.aws/config"); os.IsNotExist(err) {
	//	log.Fatal("~/.aws/config does not exist")
	//}

	// Verify that AWS_SDK_LOAD_CONFIG is set as an environment variable
	if val, found := os.LookupEnv("AWS_SDK_LOAD_CONFIG"); val != "true" || !found {
		log.Fatal("Environemnt variable AWS_SDK_LOAD_CONFIG must be defined and true")
	}

	// Verify that AWS_PROFILE is set as an environment variable
	if _, found := os.LookupEnv("AWS_PROFILE"); !found {
		log.Fatal("Environemnt variable AWS_PROFILE must be defined")
	}

	// Establish an AWS session
	sess := session.Must(session.NewSession())
	creds, _ := sess.Config.Credentials.Get()

	// Instantiate a new kinetic object configured with appropriate configs
	k, err := kinetic.New(func(c *kinetic.Config) {
		c.SetCredentials(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)
		c.SetRegion(*sess.Config.Region)
	})
	if err != nil {
		log.Fatalf("Unable to create new aws kinetic object due to: %v\n", err)
	}

	return k
}

func generateRandomStreamName() string {
	baseName := "test-"
	if cu, err := user.Current(); err == nil {
		baseName += cu.Username
	}

	return fmt.Sprintf("%s-%09d", baseName, rand.Intn(999999999))
}

func prepareKinesisStream(k *kinetic.Kinetic, streamName string) {
	if *config.Verbose {
		log.Printf("Preparing kinesis stream: [%s] ...\n", streamName)
	}

	// Determine if the kinesis stream exists
	err := k.WaitUntilStreamExists(context.TODO(), streamName,
		request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
	if err != nil {
		// If not, create a kinetic stream
		if *config.Verbose {
			log.Printf("Creating a new kinesis stream: [%s] ...\n", streamName)
		}
		err := k.CreateStream(streamName, 1)
		if err != nil {
			log.Fatalf("Unable to create new stream %s due to: %v\n", streamName, err)
		}

		// And wait until the stream is ready to go
		if *config.Verbose {
			log.Printf("Waiting for new kinesis stream: [%s]\n", streamName)
		}
		err = k.WaitUntilStreamExists(context.TODO(), streamName,
			request.WithWaiterDelay(request.ConstantWaiterDelay(2*time.Second)))
		if err != nil {
			log.Fatalf("Unable to wait until stream %s exists due to: %v\n", streamName, err)
		}
	}
}

func newKineticProducer(k *kinetic.Kinetic, streamName string) *producer.Producer {
	if *config.Verbose {
		log.Println("Creating a kinetic producer ...")
	}

	w, err := producer.NewKinesisWriter(k.Session.Config, streamName, func(kwc *producer.KinesisWriterConfig) {
		kwc.SetLogLevel(aws.LogDebug)
		kwc.SetResponseReadTimeout(time.Second)
	})
	if err != nil {
		log.Fatalf("Unable to create a new kinesis stream writer due to: %v\n", err)
	}

	p, err := producer.NewProducer(k.Session.Config, w, func(c *producer.Config) {
		c.SetBatchSize(500)
		c.SetBatchTimeout(1000 * time.Millisecond)
	})
	if err != nil {
		log.Fatalf("Unable to create a new producer due to: %v\n", err)
	}

	return p
}

func newKineticListener(k *kinetic.Kinetic, streamName string) *listener.Listener {
	if *config.Verbose {
		log.Println("Creating a kinetic listener ...")
	}

	// Determine the shard name
	shards, err := k.GetShards(streamName)
	if err != nil {
		log.Fatalf("Unable to get shards for stream %s due to: %v\n", streamName, err)
	}

	r, err := listener.NewKinesisReader(k.Session.Config, streamName, shards[0],
		func(krc *listener.KinesisReaderConfig) {
			krc.SetResponseReadTimeout(1000 * time.Millisecond)
	})

	l, err := listener.NewListener(k.Session.Config, r, func(c *listener.Config) {
		c.SetQueueDepth(20)
		c.SetConcurrency(10)
	})
	if err != nil {
		log.Fatalf("Unable to create a new listener due to: %v\n", err)
	}

	return l
}
