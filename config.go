package kinetic

import (
	"io/ioutil"
	"log"
	"os"

	"code.google.com/p/gcfg"
)

var defaultConfig string = `
[kinesis]
stream              = stream-name
shard               = 0
sharditeratortype   = 1

[aws]
accesskey           = accesskey
secretkey           = secretkey
region              = us-east-1

[debug]
verbose             = true
`

type Config struct {
	Kinesis struct {
		Host              string
		Port              string
		Stream            string
		Shard             string
		ShardIteratorType int
	}

	AWS struct {
		AccessKey string
		SecretKey string
		Region    string
	}

	Debug struct {
		Verbose bool
	}
}

func GetConfig() *Config {
	con := new(Config)

	file, err := ioutil.ReadFile("/etc/kinetic.conf")
	if err != nil {
		switch err.(type) {
		case *os.PathError:
			log.Println("Failed to parse config. Loading default configuration.")
			file = []byte(defaultConfig)
			break
		default:
			log.Println("Missing config: /etc/kinetic.conf. Loading default configuration.")
			file = []byte(defaultConfig)
			break
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
