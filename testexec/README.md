# testexec
testexec is an executable CLI program to test the kinetic producer / listener.  

The executable program creates a kinetic object connecting to either a local kinesalte instance or to AWS Kinesis using local AWS credentials/config.
  
A new stream will be created (with a random stream name) unless a stream name is provided (See usage).

There are three modes of operations permitted:  'read', 'write' and 'readwrite'.  Write mode is produce only where as read mode is consume only.  Readwrite mode is both produce and consume concurrently.   

So depending on the mode of operation, a kinetic producer will stream dummy data to the kinesis stream and a kinetic listener will stream from the kinesis stream.

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
  -cleanup
    	used to specify whether or not to delete the kinesis stream after processing is complete. (default true)
  -duration int
    	used to specify the duration (in seconds) the program should run. This flag is only applicable to 'write' and 'readwrite' modes.  Use zero or negative number to run indefinitely. (default 0)
  -location string
    	used to specify the location of the kinesis stream.  Accepted values are (local|aws).  For local, run kinesalite on http://127.0.0.1:4567. For aws, your aws credentials and configuration need to be defined at ~/.aws (default "local")
  -mode string
    	used to specify the mode in which to run; either 'r', 'read', 'w', 'write', 'rw' or 'readwrite' (default "readwrite")
  -num-msgs int
    	used to specify the number of messages to (attempt to) send.  This flag is only applicable to 'write' and 'readwrite' modes.  Use zero or a negative number to produce indefinitely (default 0)
  -stream-name string
    	used to specify a pre-existing stream to be used for testing.  A new stream will be created if not defined.
  -throttle
    	used to specify whether to throttle PutRecord requests by 1 ms.   (default true)
  -verbose
    	used to specify whether or not to log in verbose mode.  (default false)
```

## Examples

To run kinetic testexec on a local kinesalite instance to stream a fixed number of messages to a new kinesis stream:
```sh
./testexec -num-msgs 1000 -verbose
```

To run kinetic testexec on a local kinesalite instance to stream for a fixed duration of time to an existing kinesis stream:
```sh
./testexec -location local -stream-name some-stream -duration 1000
```

To run kinetic testexec on an AWS Kinesis Stream to stream indefinitely (Ctrl-C to stop producing):
```sh
./testexec -location aws -duration -1 -mode write -stream-name test-stream -cleanup=false -verbose
```

To run kinetic testexec in read mode an AWS KinesisStream:
To run kinetic testexec on an AWS Kinesis Stream to stream indefinitely (Ctrl-C to stop producing):
```sh
./testexec -location aws -mode read -stream-name test-stream -cleanup=false -verbose
```
