package kinetic

import (
	"io/ioutil"
	"os/exec"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBadConfig(t *testing.T) {
	Convey("Given an incorrectly formatted config file", t, func() {
		Convey("Panic should occur when attempting to load the config", func() {
			moveConfig(t)
			makeBadConfig(t, "/etc/kinetic.conf")
			defer func() {
				restoreConfig(t)
				So(recover(), ShouldNotResemble, nil)
			}()

			GetConfig()
		})
	})
}

func TestMissingConfig(t *testing.T) {
	Convey("Given a missing config file", t, func() {
		moveConfig(t)

		Convey("Panic should occur when attempting to load the config", func() {
			defer func() {
				restoreConfig(t)
				So(recover(), ShouldNotResemble, nil)
			}()

			GetConfig()
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
