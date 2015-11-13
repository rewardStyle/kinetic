package kinetic

import (
	"io/ioutil"

	"code.google.com/p/gcfg"
)

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

	file, err := ioutil.ReadFile("kinetic.conf")
	if err != nil {
		println("Missing config: kinetic.conf")

		// Panic because logging isn't yet configured
		panic(err)
	}

	err = gcfg.ReadStringInto(con, string(file))
	if err != nil {
		panic(err)
	}

	return con
}
