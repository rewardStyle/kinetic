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
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/rcrowley/go-metrics"
	"github.com/rewardStyle/kinetic"
	"github.com/rewardStyle/kinetic/listener"
	"github.com/rewardStyle/kinetic/message"
	"github.com/rewardStyle/kinetic/producer"

	"net/http"
	_ "net/http/pprof"
)

// Define constants for Kinesis stream location
const (
	LocationLocal = "local"
	LocationAws   = "aws"
)

// Define operation modes
const (
	ModeRead      = "read"
	ModeWrite     = "write"
	ModeReadWrite = "readwrite"
)

// Declare global variables
var streamName string
var startTime time.Time
var streamStart time.Time
var streamStop time.Time
var pipeOfDeath chan os.Signal
var stopDisplay chan struct{}
var stopProduce chan struct{}
var stopListen chan struct{}
var cfg *Config
var registry metrics.Registry

func init() {
	// Set up Http server for pprof
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Start the stopwatch
	startTime = time.Now()

	// Set the RNG Seed based on current time (in order to randomize the RNG)
	rand.Seed(startTime.UTC().UnixNano())

	// Instantiate channels for communicating between threads
	pipeOfDeath = make(chan os.Signal, 1)
	stopDisplay = make(chan struct{}, 1)
	stopProduce = make(chan struct{}, 1)
	stopListen = make(chan struct{}, 1)

	// Set up pipeOfDeath channel to receive os signals
	signal.Notify(pipeOfDeath, os.Interrupt)

	// Set up rcrowley metrics registry
	registry = metrics.NewRegistry()
}

func cleanup(k *kinetic.Kinetic, stream string) {
	if *cfg.Cleanup {
		if *cfg.Verbose {
			log.Println()
			log.Printf("Cleaning up by deleting stream [%s] ...\n", stream)
		}
		k.DeleteStream(stream)
		if *cfg.Verbose {
			log.Printf("Waiting for stream [%s] to be deleted ...\n", stream)
		}
		k.WaitUntilStreamDeleted(context.TODO(), stream,
			request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
		if *cfg.Verbose {
			log.Println("Clean up complete")
			log.Println()
		}
	}
}

func main() {
	// Process command line arguments
	cfg = parseCommandLineArgs()
	cfg.printConfigs()

	// Instantiate a new kinetic object based on the location of the kinesis stream
	var k *kinetic.Kinetic
	switch strings.ToLower(*cfg.Location) {
	case LocationLocal:
		k = newDefaultKinetic()
	case LocationAws:
		k = newAwsKinetic()
	default:
		log.Fatalf("Unknown location for kinesis stream: %s\n", *cfg.Location)
	}

	// Set streamName from config or generate a random one
	streamName = *cfg.StreamName
	if streamName == "" {
		streamName = generateRandomStreamName()
	}

	// Create and wait for a new kinetic stream (if the stream name doesn't already exist)
	prepareKinesisStream(k, streamName)
	defer cleanup(k, streamName)

	// Create a new kinetic producer
	p := newKineticProducer(k, streamName)

	// Create a new kinetic listener
	l := newKineticListener(k, streamName)

	// Instantiate StreamData Object to keep stats
	streamData := NewStreamData()

	// Run all the things concurrently
	mainWg := sync.WaitGroup{}
	mainWg.Add(3) // Wait for display, produce and listen
	go handlePoD()
	go display(streamData, p, l, &mainWg)
	go produce(streamData, p, &mainWg)
	go listen(streamData, l, &mainWg)
	mainWg.Wait()
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
	if *cfg.Verbose {
		log.Printf("Preparing kinesis stream: [%s] ...\n", streamName)
	}

	// Determine if the kinesis stream exists
	err := k.WaitUntilStreamExists(context.TODO(), streamName,
		request.WithWaiterDelay(request.ConstantWaiterDelay(1*time.Second)))
	if err != nil {
		// If not, create a kinetic stream
		if *cfg.Verbose {
			log.Printf("Creating a new kinesis stream: [%s] ...\n", streamName)
		}
		err := k.CreateStream(streamName, 1)
		if err != nil {
			log.Fatalf("Unable to create new stream %s due to: %v\n", streamName, err)
		}

		// And wait until the stream is ready to go
		if *cfg.Verbose {
			log.Printf("Waiting for new kinesis stream: [%s] ...\n", streamName)
		}
		err = k.WaitUntilStreamExists(context.TODO(), streamName,
			request.WithWaiterDelay(request.ConstantWaiterDelay(2*time.Second)))
		if err != nil {
			log.Fatalf("Unable to wait until stream %s exists due to: %v\n", streamName, err)
		}
	}
}

func newKineticProducer(k *kinetic.Kinetic, streamName string) *producer.Producer {
	if *cfg.Verbose {
		log.Println("Creating a kinetic producer ...")
	}

	psc := producer.NewDefaultStatsCollector(registry)
	w, err := producer.NewKinesisWriter(k.Session.Config, streamName, func(kwc *producer.KinesisWriterConfig) {
		kwc.SetLogLevel(aws.LogDebug)
		kwc.SetResponseReadTimeout(time.Second)
		kwc.SetStatsCollector(psc)
	})
	if err != nil {
		log.Fatalf("Unable to create a new kinesis stream writer due to: %v\n", err)
	}

	p, err := producer.NewProducer(k.Session.Config, w, func(c *producer.Config) {
		c.SetBatchSize(500)
		c.SetBatchTimeout(1000 * time.Millisecond)
		c.SetConcurrency(10)
		c.SetMaxRetryAttempts(2)
		c.SetQueueDepth(100)
		c.SetStatsCollector(psc)
	})
	if err != nil {
		log.Fatalf("Unable to create a new producer due to: %v\n", err)
	}

	return p
}

func newKineticListener(k *kinetic.Kinetic, streamName string) *listener.Listener {
	if *cfg.Verbose {
		log.Println("Creating a kinetic listener ...")
	}

	// Determine the shard name
	shards, err := k.GetShards(streamName)
	if err != nil {
		log.Fatalf("Unable to get shards for stream %s due to: %v\n", streamName, err)
	}

	lsc := listener.NewDefaultStatsCollector(registry)
	r, err := listener.NewKinesisReader(k.Session.Config, streamName, shards[0],
		func(krc *listener.KinesisReaderConfig) {
			krc.SetResponseReadTimeout(1000 * time.Millisecond)
			krc.SetStatsCollector(lsc)
		})
	if err != nil {
		log.Fatalf("Unable to create a new kinesis reader due to: %v\n", err)
	}

	l, err := listener.NewListener(k.Session.Config, r, func(c *listener.Config) {
		c.SetQueueDepth(500)
		c.SetConcurrency(10)
		c.SetStatsCollector(lsc)
	})
	if err != nil {
		log.Fatalf("Unable to create a new listener due to: %v\n", err)
	}

	return l
}

func handlePoD() {
	<-pipeOfDeath
	if *cfg.Verbose {
		log.Println()
		log.Println("main: Received pipeOfDeath ...")
	}
	if *cfg.Mode == ModeRead {
		stopListen <- struct{}{}
	} else {
		stopProduce <- struct{}{}
	}
}

func display(sd *StreamData, p *producer.Producer, l *listener.Listener, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-stopDisplay:
			if *cfg.Verbose {
				log.Println()
				log.Print("display: Received stopDisplay ...")
			}
			log.Println()
			log.Printf("Stream name: %s\n", streamName)
			log.Printf("Elapsed Time: %v\n", time.Since(startTime))
			if !streamStart.IsZero() && !streamStop.IsZero() {
				log.Printf("Streaming Time: %v\n", streamStop.Sub(streamStart))
			}
			log.Println()
			log.Println("***** Stream Data Summary *****")
			if *cfg.Mode != ModeRead {
				p.Stats.(*producer.DefaultStatsCollector).PrintStats()
			}
			if *cfg.Mode != ModeWrite {
				l.Stats.(*listener.DefaultStatsCollector).PrintStats()
				sd.printSummary()
			}
			return
		case <-time.After(time.Second):
			log.Println()
			log.Printf("Stream name: %s\n", streamName)
			log.Printf("Elapsed Time: %v\n", time.Since(startTime))
			log.Println()
			log.Println("***** Stream Data Stats *****")
			if *cfg.Mode != ModeRead {
				p.Stats.(*producer.DefaultStatsCollector).PrintStats()
			}
			if *cfg.Mode != ModeWrite {
				l.Stats.(*listener.DefaultStatsCollector).PrintStats()
				sd.printStats()
			}
		}
	}
}

func produce(sd *StreamData, p *producer.Producer, wg *sync.WaitGroup) {
	defer wg.Done()

	// Return early if we don't need to produce
	if *cfg.Mode == ModeRead {
		return
	}

	defer func() {
		if *cfg.Verbose {
			log.Println()
			log.Println("producer: Exiting produce ...")
		}

		// In write mode the producer controls when to stop displaying
		if *cfg.Mode == ModeWrite {
			stopDisplay <- struct{}{}
		}
	}()

	// Define a timeout channel if the duration is set
	streamStart = time.Now()
	var timeout <-chan time.Time
	if cfg.Duration != nil && *cfg.Duration > 0 {
		timeout = time.After(time.Duration(*cfg.Duration) * time.Second)
	} else {
		timeout = make(chan time.Time, 1)
	}

	// Run Send in a separate go routine listening for the sendSignal
	var sendSignal = make(chan struct{}, 1)
	go func() {
		for {
			<-sendSignal
			jsonStr, _ := json.Marshal(NewMessage())
			if err := p.Send(&message.Message{
				PartitionKey: aws.String("key"),
				Data:         []byte(jsonStr),
			}); err == nil {
				sd.incrementMsgCount()
			} else {
				log.Println("producer: Uh oh, something bad happened!!!!")
			}
		}
	}()

	// Control when to exit produce
	produceWg := sync.WaitGroup{}
	produceWg.Add(1)
	go func() {
		defer produceWg.Done()

		var sent uint64
		var sendTicker *time.Ticker
		if *cfg.Throttle {
			sendTicker = time.NewTicker(time.Millisecond)
		} else {
			sendTicker = time.NewTicker(time.Nanosecond)
		}
	produce:
		for {
			select {
			case <-stopProduce:
				if *cfg.Verbose {
					log.Println()
					log.Println("producer: Received stop produce ...")
				}
				break produce
			case <-timeout:
				if *cfg.Verbose {
					log.Println()
					log.Print("producer: Duration time out ...")
				}
				break produce
			case <-sendTicker.C:
				// Break from the loop if we have sent the correct number of messages
				if cfg.NumMsgs != nil {
					if atomic.LoadUint64(&sent) >= uint64(*cfg.NumMsgs) {
						break produce
					}
				}
				sendSignal <- struct{}{}
				atomic.AddUint64(&sent, 1)
			}
		}
		streamStop = time.Now()

		// We may need to wait for Send to finish so we add a delay before exiting produce
		var staleTimeout time.Duration
		switch strings.ToLower(*cfg.Location) {
		case LocationLocal:
			staleTimeout = time.Duration(2 * time.Second)
		case LocationAws:
			staleTimeout = time.Duration(10 * time.Second)
		}
		staleTime := time.NewTimer(staleTimeout)

		for {
			select {
			case <-staleTime.C:
				if *cfg.Verbose {
					log.Println()
					log.Println("producer: No more outgoing messages from producer ...")
				}
				return
			case <-time.After(time.Second):
				newSent := p.Stats.(*producer.DefaultStatsCollector).SentSuccess.Count()
				if sent != uint64(newSent) {
					staleTime.Reset(staleTimeout)
					sent = uint64(newSent)
				}
			}
		}
	}()
	produceWg.Wait()
}

func listen(sd *StreamData, l *listener.Listener, wg *sync.WaitGroup) {
	defer wg.Done()

	// Return early if we don't need to produce
	if *cfg.Mode == ModeWrite {
		return
	}

	defer func() {
		if *cfg.Verbose {
			log.Println()
			log.Println("listener: Exiting listen ...")
		}

		// In read and readwrite mode the listener controls when to stop displaying
		stopDisplay <- struct{}{}
	}()

	// Call Listen within a go routine
	go func() {
		l.Listen(func(m *message.Message, wg *sync.WaitGroup) error {
			defer wg.Done()

			// Unmarshal data
			msg := &Message{}
			json.Unmarshal(m.Data, msg)

			// Only mark "done" if the message isn't a duplicate
			if sd.exists(msg.ID) {
				if *cfg.Verbose {
					log.Printf("listener: Duplicate message: %v\n", msg)
				}
			}

			// Record message regardless if it is a duplicate
			sd.mark(msg.ID)

			return nil
		})
	}()

	// Control when to exit listen
	listenWg := sync.WaitGroup{}
	listenWg.Add(1)
	go func() {
		defer listenWg.Done()

		var staleTimeout time.Duration
		switch strings.ToLower(*cfg.Location) {
		case LocationLocal:
			staleTimeout = time.Duration(3 * time.Second)
		case LocationAws:
			staleTimeout = time.Duration(60 * time.Second)
		}
		staleTime := time.NewTimer(staleTimeout)

		var consumed uint64
		for {
			select {
			case <-stopListen:
				if *cfg.Verbose {
					log.Println()
					log.Println("listener: Received stop listen ...")
				}
				return
			case <-staleTime.C:
				if *cfg.Verbose {
					log.Println()
					log.Println("listener: No more incoming messages from listener ...")
				}
				return
			case <-time.After(time.Second):
				newConsumed := l.Stats.(*listener.DefaultStatsCollector).Consumed.Count()
				if consumed != uint64(newConsumed) {
					staleTime.Reset(staleTimeout)
					consumed = uint64(newConsumed)
				}
			}
		}
	}()
	listenWg.Wait()
}
