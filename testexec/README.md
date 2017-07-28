# testexec
testexec is an executable CLI program to test the kinetic producer / consumer.  

The executable program creates a kinetic object connecting to either a local kinesalte instance or to AWS Kinesis using local AWS credentials/config.
  
A new stream will be created (with a random stream name) unless a stream name is provided (See usage).

There are three modes of operations permitted:  'read', 'write' and 'readwrite'.  Write mode is produce only where as read mode is consume only.  Readwrite mode is both produce and consume concurrently.   

So depending on the mode of operation, a kinetic producer will stream dummy data to the kinesis stream and a kinetic consumer will stream from the kinesis stream.

The program logs stream data stats to the console log periodically.

## Installation
```sh
cd ${GOHOME}/src/github.com/rewardStyle/kinetic/testexec`
go install
```

## Requirements

- local
    - requires kinesalite running on http://127.0.0.1:4567

- aws
    - ~/.aws/credentials to exist and contain valid aws credentials
    - ~/.aws/config to exist and contain AWS configuration settings
    - environment variable `AWS_SDK_LOAD_CONFIG` should exist and be set to `true`
    - environment variable `AWS_PROFILE` should exist and be set to the preferred AWS profile
        

## Usage

```text
Usage of ./testexec:
  -blast
    	used to specify whether to call the producer's send function at full blast. (default false)
  -clean
    	used to specify whether or not to delete the kinesis stream after processing is complete. (default true)
  -count int
    	used to specify the number of messages to (attempt to) send.  This flag is only applicable to 'write' and 'readwrite' modes.  Use zero or a negative number to produce indefinitely
  -duration int
    	used to specify the duration (in seconds) the program should run. This flag is only applicable to 'write' and 'readwrite' modes.  Use zero or negative number to run indefinitely.
  -location string
    	used to specify the location of the kinesis stream.  Accepted values are (local|aws).  For local, run kinesalite on http://127.0.0.1:4567. For aws, your aws credentials and configuration need to be defined at ~/.aws (default "local")
  -mode string
    	used to specify the mode in which to run; either 'r', 'read', 'w', 'write', 'rw' or 'readwrite' (default "readwrite")
  -stream string
    	used to specify a specific stream to write to / read from for testing. The stream will be created if a stream name is given but does not exist.  A random stream will be created if the stream is undefined.
  -verbose
      	used to specify whether or not to log in verbose mode (default true)
```

## Examples

To run kinetic testexec to stream to / from a local kinesalite instance indefinitely to a new Kinesis stream:
```sh
./testexec
```

To run kinetic testexec to stream a fixed number of messages to / from a new Kinesis stream:
```sh
./testexec -location aws -count 1000
```

To run a stress test on the kinetic testexec to stream to / from a specific Kinesis stream and save the stream for confirmation later (Ctrl-C to stop producing):
```sh
./testexec -location aws -stream my-stress-test-stream -blast=true -clean=false
```

To run kinetic testexec in listen-only mode on an existing Kinesis stream:
```sh
./testexec -location aws -mode read -stream my-stress-test-stream -clean=false
```
