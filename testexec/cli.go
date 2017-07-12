package main

import (
	"flag"
	"log"
	"strings"
)

// Config is a data structure used to hold this program's configuration info
type Config struct {
	Mode       *string
	Location   *string
	StreamName *string
	NumMsgs    *int
	Duration   *int
	Throttle   *bool
	Deadlock   *bool
	Cleanup    *bool
	Verbose    *bool
}

func parseCommandLineArgs() *Config {

	// Define command line flags
	modePtr := flag.String("mode", "readwrite", "used to specify the mode in which to run; either 'r', 'read', 'w', "+
		"'write', 'rw' or 'readwrite'")
	locationPtr := flag.String("location", "local", "used to specify the location of the kinesis stream.  "+
		"Accepted values are (local|aws).  For local, run kinesalite on http://127.0.0.1:4567. For aws, your "+
		"aws credentials and configuration need to be defined at ~/.aws")
	streamNamePtr := flag.String("stream-name", "", "used to specify a pre-existing stream to be used for "+
		"testing.  A new stream will be created if not defined.")
	numMsgsPtr := flag.Int("num-msgs", 0, "used to specify the number of messages to (attempt to) send.  This "+
		"flag is only applicable to 'write' and 'readwrite' modes.  Use zero or a negative number to produce "+
		"indefinitely")
	durationPtr := flag.Int("duration", 0, "used to specify the duration (in seconds) the program should run. "+
		"This flag is only applicable to 'write' and 'readwrite' modes.  Use zero or negative number to run "+
		"indefinitely.")
	throttlePtr := flag.Bool("throttle", true, "used to specify whether to throttle PutRecord requests by 1 ms.")
	deadlockPtr := flag.Bool("deadlock", false, "used to test potential deadlock condition for the producer.")
	cleanupPtr := flag.Bool("cleanup", true, "used to specify whether or not to delete the kinesis stream after "+
		"processing is complete.")
	verbosePtr := flag.Bool("verbose", true, "used to specify whether or not to log in verbose mode")

	// Parse command line arguments
	flag.Parse()

	// Process command line arguments
	if *durationPtr <= 0 {
		durationPtr = nil
	}

	if *numMsgsPtr <= 0 {
		numMsgsPtr = nil
	}

	var mode string
	switch strings.ToLower(*modePtr) {
	case "r":
		fallthrough
	case "read":
		mode = ModeRead
	case "w":
		fallthrough
	case "write":
		mode = ModeWrite
	case "rw":
		fallthrough
	case "readwrite":
		mode = ModeReadWrite
	default:
		log.Fatal("Mode must be defined as either 'r', 'read', 'w', 'write', 'rw' or 'readwrite'")
	}

	return &Config{
		Mode:       &mode,
		StreamName: streamNamePtr,
		Duration:   durationPtr,
		NumMsgs:    numMsgsPtr,
		Location:   locationPtr,
		Throttle:   throttlePtr,
		Deadlock:   deadlockPtr,
		Cleanup:    cleanupPtr,
		Verbose:    verbosePtr,
	}
}

func (c *Config) printConfigs() {
	if *c.Verbose {
		log.Println("Command Line Arguments:")
		log.Println("-mode: ", *c.Mode)
		log.Println("-location: ", *c.Location)
		if len(*c.StreamName) == 0 {
			log.Println("-stream-name: (randomly generated)")
		} else {
			log.Println("-stream-name: ", *c.StreamName)
		}
		if c.NumMsgs != nil {
			log.Println("-num-msgs: ", *c.NumMsgs)
		} else {
			log.Println("-num-msgs: (unbounded)")
		}
		if c.Duration != nil {
			log.Printf("-duration: [%d] (s)", *c.Duration)
		} else {
			log.Println("-duration: (indefinite)")
		}
		log.Println("-throttle: ", *c.Throttle)
		log.Println("-deadlock: ", *c.Deadlock)
		log.Println("-cleanup: ", *c.Cleanup)
		log.Println("-verbose: ", *c.Verbose)
		log.Println()
	}
}
