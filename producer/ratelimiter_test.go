package producer

import (
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"
)

func TestRateLimiter(t *testing.T) {
	limit := 1000
	duration := time.Second
	Convey("calling newRateLimiter should return the pointer to a rateLimiter with properly initalized values", t, func() {
		rl := newRateLimiter(limit, duration)
		So(rl, ShouldNotBeNil)
		So(rl.limit, ShouldEqual, limit)
		So(rl.duration, ShouldEqual, duration)
		So(rl.tokenCount, ShouldEqual, limit)

		Convey("calling getTokenCount returns the correct token count", func() {
			So(rl.getTokenCount(), ShouldEqual, limit)
		})

		Convey("calling claimTokens reduces the tokens properly", func() {
			So(rl.getTokenCount(), ShouldEqual, limit)
			claim := 100
			rl.claimTokens(claim)
			So(rl.getTokenCount(), ShouldEqual, limit - claim)
		})

		Convey("calling reset will reset the token count back to the original limit", func() {
			So(rl.getTokenCount(), ShouldEqual, limit)
			claim := 100
			rl.claimTokens(claim)
			So(rl.getTokenCount(), ShouldNotEqual, limit)
			rl.reset()
			So(rl.getTokenCount(), ShouldEqual, limit)
		})

		Convey("calling start on the rateLimiter", func() {
			// TODO: Finish this unit test
			rl.start()
			time.Sleep(10 * time.Second)
		})

		Convey("calling stop on the rateLimiter", func() {
			// TODO: Finish this unit test
		})
	})
}
