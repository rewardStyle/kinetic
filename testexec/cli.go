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
	Cleanup    *bool
	Verbose    *bool
}

func parseCommandLineArgs() *Config {

	// Define command line flags
	modePtr := flag.String("mode", "write", "used to specify the mode in which to run; either read or write")
	locationPtr := flag.String("location", "local", "used to specify the location of the kinesis stream.  " +
		"Accepted values are (local|aws).  For local, run kinesalite on http://127.0.0.1:4567. For aws, your " +
		"aws credentials and configuration need to be defined at ~/.aws")
	streamNamePtr := flag.String("stream-name", "", "used to specify a pre-existing stream to be used for " +
		"testing.  A new stream will be created if not defined.")
	numMsgsPtr := flag.Int("num-msgs", 0, "used to specify the number of messages to (attempt to) send / " +
		"receive.  Either -num-msgs or -duration must be set.")
	durationPtr := flag.Int("duration", 0, "used to specify the duration (in seconds) the program should run. " +
		"Use a value of -1 to run indefinitely.  Either -num-msgs or -duration must be set.")
	cleanupPtr := flag.Bool("cleanup", true, "used to specify whether or not to delete a newly created kinesis " +
		"stream")
	verbosePtr := flag.Bool("verbose", false, "used to specify whether or not to log in verbose mode")

	// Parse command line arguments
	flag.Parse()

	// Process command line arguments
	if *numMsgsPtr == 0 && *durationPtr == 0 {
		log.Fatal("Either -num-msgs or -duration must be set.")
	} else if *numMsgsPtr != 0 && *durationPtr != 0 {
		log.Fatal("Both -num-msgs and -duration were set.  Only one may be set.")
	} else if *durationPtr != 0 {
		numMsgsPtr = nil
	} else if *numMsgsPtr < 0 {
		log.Fatal("Number of messages value must be greater than 0")
	} else {
		durationPtr = nil
	}

	var mode string
	switch strings.ToLower(*modePtr) {
	case "read":
		fallthrough
	case "write":
		mode = strings.ToLower(*modePtr)
	default:
		log.Fatal("Mode must be defined as either 'read' or 'write'")
	}

	return &Config{
		Mode: &mode,
		StreamName: streamNamePtr,
		Duration: durationPtr,
		NumMsgs: numMsgsPtr,
		Location: locationPtr,
		Cleanup: cleanupPtr,
		Verbose: verbosePtr,
	}
}

func (c *Config) printConfigs() {
	if *c.Verbose {
		log.Println("Command Line Arguments:")
		log.Println("-mode: ", *c.Mode)
		log.Println("-location: ", *c.Location)
		log.Println("-stream-name: ", *c.StreamName)
		if c.NumMsgs != nil {
			log.Println("-num-msgs: ", *c.NumMsgs)
		}
		if c.Duration != nil {
			log.Println("-duration: ", *c.Duration)
		}
		log.Println("-cleanup: ", *c.Cleanup)
		log.Println("-verbose: ", *c.Verbose)
		log.Println()
	}
}
