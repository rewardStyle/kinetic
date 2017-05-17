package kinetic

import (
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/gcfg.v1"
)

var (
	configPath = "/etc/kinetic.conf"

	defaultConfig = `
[kinesis]
stream              = stream-name
shard               = 0
sharditeratortype   = 3

[firehose]
stream              = firehose-stream-name

[aws]
accesskey           = accesskey
secretkey           = secretkey
region              = us-east-1

[debug]
verbose             = true

[concurrency]
listener            = 100
producer            = 100
`
)

type config struct {
	Kinesis struct {
		Stream            string
		Shard             string
		ShardIteratorType int
	}

	Firehose struct {
		Stream string
	}

	AWS struct {
		AccessKey string
		SecretKey string
		Region    string
	}

	Debug struct {
		Verbose bool
	}

	Concurrency struct {
		Listener int
		Producer int
	}
}

func getConfig() *config {
	con := new(config)

	file, err := ioutil.ReadFile(configPath)
	if err != nil {
		switch err.(type) {
		case *os.PathError:
			log.Println("Failed to parse config. Loading default configuration.")
			file = []byte(defaultConfig)
		default:
			log.Println("Missing config: " + configPath + ". Loading default configuration.")
			file = []byte(defaultConfig)
		}
	}

	err = gcfg.ReadStringInto(con, string(file))
	if err != nil {
		log.Println("Failed to parse config. Loading default configuration.")
		err = gcfg.ReadStringInto(con, string(defaultConfig))
		if err != nil {
			panic(err)
		}
	}

	return con
}
