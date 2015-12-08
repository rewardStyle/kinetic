package kinetic

import (
	"io/ioutil"
	"os/exec"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBadConfig(t *testing.T) {
	Convey("Given an incorrectly formatted config file", t, func() {
		Convey("The default configuration should be loaded", func() {
			defer func() {
				restoreConfig(t)
				t.Fatalf("Something went wrong")
			}()

			moveConfig(t)
			makeBadConfig(t, "/etc/kinetic.conf")

			config := GetConfig()

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
			defer func() {
				restoreConfig(t)
				t.Fatalf("Something went wrong")
			}()

			config := GetConfig()

			So(config.Kinesis.Stream, ShouldResemble, "stream-name")
			So(config.AWS.AccessKey, ShouldResemble, "accesskey")
			So(config.AWS.SecretKey, ShouldResemble, "secretkey")
			restoreConfig(t)
		})
	})
}

func moveConfig(t *testing.T) {
	err := exec.Command("mv", "/etc/kinetic.conf", "/etc/kinetic_missing.conf").Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func restoreConfig(t *testing.T) {
	err := exec.Command("mv", "/etc/kinetic_missing.conf", "/etc/kinetic.conf").Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func makeBadConfig(t *testing.T, path string) {
	err := ioutil.WriteFile(path, []byte("bad=config"), 0644)
	if err != nil {
		t.Fatalf(err.Error())
	}
}
