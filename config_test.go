package kinetic

import (
	"io/ioutil"
	"os/exec"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const testConfigPath = "kinetic.conf"

func TestBadConfig(t *testing.T) {
	Convey("Given an incorrectly formatted config file", t, func() {
		moveConfig(t)

		Convey("The default configuration should be loaded", func() {
			makeBadConfig(t, testConfigPath)

			config := getConfig()

			So(config.Kinesis.Stream, ShouldNotResemble, nil)
			So(config.AWS.AccessKey, ShouldNotResemble, nil)
			So(config.AWS.SecretKey, ShouldNotResemble, nil)
			restoreConfig(t)
		})
	})
}

func TestMissingConfig(t *testing.T) {
	Convey("Given a missing config file", t, func() {
		moveConfig(t)

		Convey("The default configuration should be loaded", func() {
			config := getConfig()

			So(config.Kinesis.Stream, ShouldNotResemble, nil)
			So(config.AWS.AccessKey, ShouldNotResemble, nil)
			So(config.AWS.SecretKey, ShouldNotResemble, nil)
			restoreConfig(t)
		})
	})
}

func moveConfig(t *testing.T) {
	exec.Command("mv", testConfigPath, testConfigPath+".missing").Run()
}

func restoreConfig(t *testing.T) {
	exec.Command("mv", testConfigPath+".missing", testConfigPath).Run()
}

func makeBadConfig(t *testing.T, path string) {
	err := ioutil.WriteFile(path, []byte("bad=config"), 0644)
	if err != nil {
		t.Fatalf(err.Error())
	}
}
