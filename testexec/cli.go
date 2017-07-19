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
	MsgCount   *int
	Duration   *int
	Blast      *bool
	Clean      *bool
	Verbose    *bool
}

func parseCommandLineArgs() *Config {

	// Define command line flags
	modePtr := flag.String("mode", "readwrite", "used to specify the mode in which to run; either 'r', 'read', 'w', "+
		"'write', 'rw' or 'readwrite'")
	locationPtr := flag.String("location", "local", "used to specify the location of the kinesis stream.  "+
		"Accepted values are (local|aws).  For local, run kinesalite on http://127.0.0.1:4567. For aws, your "+
		"aws credentials and configuration need to be defined at ~/.aws")
	streamNamePtr := flag.String("stream", "", "used to specify a specific stream to write to / read from for " +
		"testing. The stream will be created if a stream name is given but does not exist.  A random stream will " +
		"be created if the stream is undefined.")
	msgCountPtr := flag.Int("count", 0, "used to specify the number of messages to (attempt to) send.  This "+
		"flag is only applicable to 'write' and 'readwrite' modes.  Use zero or a negative number to produce "+
		"indefinitely")
	durationPtr := flag.Int("duration", 0, "used to specify the duration (in seconds) the program should run. "+
		"This flag is only applicable to 'write' and 'readwrite' modes.  Use zero or negative number to run "+
		"indefinitely.")
	blastPtr := flag.Bool("blast", false, "used to specify whether to call the producer's send function at full blast.")
	cleanPtr := flag.Bool("clean", true, "used to specify whether or not to delete the kinesis stream after "+
		"processing is complete.")
	verbosePtr := flag.Bool("verbose", true, "used to specify whether or not to log in verbose mode")

	// Parse command line arguments
	flag.Parse()

	// Process command line arguments
	if *durationPtr <= 0 {
		durationPtr = nil
	}

	if *msgCountPtr <= 0 {
		msgCountPtr = nil
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
		MsgCount:    msgCountPtr,
		Location:   locationPtr,
		Blast:      blastPtr,
		Clean:      cleanPtr,
		Verbose:    verbosePtr,
	}
}

func (c *Config) printConfigs() {
	if *c.Verbose {
		log.Println("Command Line Arguments:")
		log.Println("-mode: ", *c.Mode)
		log.Println("-location: ", *c.Location)
		if len(*c.StreamName) == 0 {
			log.Println("-stream: (randomly generated)")
		} else {
			log.Println("-stream: ", *c.StreamName)
		}
		if c.MsgCount != nil {
			log.Println("-count: ", *c.MsgCount)
		} else {
			log.Println("-count: (unbounded)")
		}
		if c.Duration != nil {
			log.Printf("-duration: [%d] (s)", *c.Duration)
		} else {
			log.Println("-duration: (indefinite)")
		}
		log.Println("-blast: ", *c.Blast)
		log.Println("-cleanup: ", *c.Clean)
		log.Println("-verbose: ", *c.Verbose)
		log.Println()
	}
}
