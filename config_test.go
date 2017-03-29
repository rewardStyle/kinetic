package kinetic

import (
	. "github.com/smartystreets/goconvey/convey"

	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/rewardStyle/kinetic/logging"
)

func getSession(config *Config) *session.Session {
	sess, err := config.GetSession()
	So(err, ShouldBeNil)
	So(sess, ShouldNotBeNil)
	return sess
}

func TestNewConfig(t *testing.T) {
	Convey("given a new kinetic config", t, func() {
		config := NewConfig()

		Convey("check the default values for its non-zero config", func() {
			So(config.awsConfig.HTTPClient.Timeout, ShouldEqual, 5*time.Minute)
		})

		Convey("check that we can retrieve an aws.Session from it ", func() {
			getSession(config)
		})

		Convey("check that we can set credentials", func() {
			config = config.WithCredentials("access-key", "secret-key", "security-token")
			sess := getSession(config)
			creds, err := sess.Config.Credentials.Get()
			So(err, ShouldBeNil)
			So(creds.AccessKeyID, ShouldEqual, "access-key")
			So(creds.SecretAccessKey, ShouldEqual, "secret-key")
			So(creds.SessionToken, ShouldEqual, "security-token")
		})

		Convey("check that we can set the region", func() {
			config = config.WithRegion("my-region")
			sess := getSession(config)
			So(aws.StringValue(sess.Config.Region), ShouldEqual, "my-region")
		})

		Convey("check that we can set the endpoint", func() {
			config = config.WithEndpoint("my-endpoint")
			sess := getSession(config)
			So(aws.StringValue(sess.Config.Endpoint), ShouldEqual, "my-endpoint")
		})

		Convey("check that we can configure a logger", func() {
			var logs []string
			loggerFn := func(args ...interface{}) {
				logs = append(logs, fmt.Sprint(args...))
			}
			config = config.WithLogger(aws.LoggerFunc(loggerFn))
			sess := getSession(config)

			Convey("check that basic logging should work", func() {
				sess.Config.Logger.Log("one")
				sess.Config.Logger.Log("two")
				sess.Config.Logger.Log("three")
				So(len(logs), ShouldEqual, 3)
				So(logs, ShouldContain, "one")
				So(logs, ShouldContain, "two")
				So(logs, ShouldContain, "three")
				Reset(func() {
					logs = nil
				})
			})
		})

		Convey("check that the default log level is off for both the sdk and kinetic", func() {
			sess := getSession(config)
			So(sess.Config.LogLevel.Value(), ShouldEqual, aws.LogOff)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeFalse)
			So(config.logLevel.Value(), ShouldEqual, aws.LogOff)
			So(config.logLevel.AtLeast(aws.LogDebug), ShouldBeFalse)
		})

		Convey("check that we can set both the sdk and kinetic log level", func() {
			ll := aws.LogDebug | aws.LogDebugWithSigning | logging.LogDebug
			config = config.WithLogLevel(ll)
			sess := getSession(config)
			So(sess.Config.LogLevel.AtLeast(aws.LogDebug), ShouldBeTrue)
			So(sess.Config.LogLevel.Matches(aws.LogDebugWithSigning), ShouldBeTrue)
			So(config.logLevel.AtLeast(logging.LogDebug), ShouldBeTrue)
		})

		Convey("check that we can set the http.Client Timeout", func() {
			config = config.WithHttpClientTimeout(10 * time.Minute)
			So(config.awsConfig.HTTPClient.Timeout, ShouldEqual, 10*time.Minute)
		})
	})
}
