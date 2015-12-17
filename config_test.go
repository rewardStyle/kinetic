package kinetic

import (
	"io/ioutil"
	"os/exec"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBadConfig(t *testing.T) {
	Convey("Given an incorrectly formatted config file", t, func() {
		moveConfig(t)

		Convey("The default configuration should be loaded", func() {
			makeBadConfig(t, "kinetic.conf")

			config := getConfig()

			So(config.Kinesis.Stream, ShouldResemble, "stream-name")
			So(config.AWS.AccessKey, ShouldResemble, "accesskey")
			So(config.AWS.SecretKey, ShouldResemble, "secretkey")
			restoreConfig(t)
		})
	})
}

func TestMissingConfig(t *testing.T) {
	Convey("Given a missing config file", t, func() {
		moveConfig(t)

		Convey("The default configuration should be loaded", func() {
			config := getConfig()

			So(config.Kinesis.Stream, ShouldResemble, "stream-name")
			So(config.AWS.AccessKey, ShouldResemble, "accesskey")
			So(config.AWS.SecretKey, ShouldResemble, "secretkey")
			restoreConfig(t)
		})
	})
}

func moveConfig(t *testing.T) {
	exec.Command("mv", configPath, configPath+".missing").Run()
}

func restoreConfig(t *testing.T) {
	exec.Command("mv", configPath+".missing", configPath).Run()
}

func makeBadConfig(t *testing.T, path string) {
	err := ioutil.WriteFile(path, []byte("bad=config"), 0644)
	if err != nil {
		t.Fatalf(err.Error())
	}
}
