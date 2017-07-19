package producer

import (
	"sync"
	"time"
)

// TODO: add size rate limiting in addition to message count

// rateLimiter is used by the producer to rate limit the total number and size of records sent per cycle.
type rateLimiter struct {
	limit        int           // upper limit of throughput per cycle
	duration     time.Duration // frequency with which to reset the remaining tokens count
	tokenCount   int           // remaining tokens available for the cycle
	tokenMu      sync.Mutex    // mutex to protect remaining token count
	stopChannel  chan empty    // channel for communicating when to stop rate limiting
	resetChannel chan empty    // channel for communicating when the rate limiter has been reset
	startOnce    sync.Once     // startOnce is used to ensure that start is called once and only once
	stopOnce     sync.Once     // stopOnce is used to ensure that stop is called once and only once
}

// newRateLimiter creates a new rateLimiter.
func newRateLimiter(limit int, duration time.Duration) *rateLimiter {
	return &rateLimiter{
		limit: limit,
		duration: duration,
		tokenCount: limit,
	}
}

// start runs a timer in a go routine background which resets the the number and size counters every cycle.
func (r *rateLimiter) start() {
	r.startOnce.Do(func() {
		// Reset stopOnce to allow the rateLimiter to be shut down again
		r.stopOnce = sync.Once{}

		r.stopChannel = make(chan empty)
		r.resetChannel = make(chan empty)
		ticker := time.NewTicker(r.duration)
		go func(){
			for {
				select {
				case <-r.stopChannel:
					ticker.Stop()
					return
				case <-ticker.C:
					r.reset()
				}
			}
		}()
	})
}

// stop sends a signal to the rateLimiter's stopChannel
func (r *rateLimiter) stop() {
	r.stopOnce.Do(func(){
		r.stopChannel <- empty{}

		close(r.stopChannel)
		close(r.resetChannel)

		// Reset startOnce to allow the rateLimiter to be started again
		r.startOnce = sync.Once{}
	})
}

// restart calls stop then start to restart the rate limiter
func (r *rateLimiter) restart() {
	r.stop()
	r.start()
}

// resize updates the maximum message size and reset frequency
func (r *rateLimiter) resize(limit int, duration time.Duration) {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	r.limit = limit
	r.duration = duration

	r.restart()
}

// reset is called to reset the rateLimiter's tokens to the initial values
func (r *rateLimiter) reset() {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	r.tokenCount = r.limit

	// Send a signal to the resetChanel and remove it immediately if no one picked it up
	select {
	case r.resetChannel <-empty{}:
	default:
		select {
		case <-r.resetChannel:
		default:
		}
	}
}

// getTokenCount is used to retrieve the current token count.  Be aware of thread safety when trying to use
// getTokenCount and claimToken back to back as they are not tread safe with each other (use tryToClaimToken instead)
func (r *rateLimiter) getTokenCount() int {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	return r.tokenCount
}

// claimTokens is used to claim tokens prior to sending messages. Be aware of thread safety when trying to use
// getTokenCount and claimToken back to back as they are not tread safe with each other (use tryToClaimToken instead)
func (r *rateLimiter) claimTokens(count int) {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	r.tokenCount -= count
}


// tryToClaim attempts to claim the wanted number of tokens and returns the actual number claimed based on the
// number of tokens actually availab
func (r *rateLimiter) tryToClaimTokens(want int) (got int) {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()

	if want <= r.tokenCount {
		r.tokenCount -= want
		got = want
	} else {
		r.tokenCount = 0
		got = r.tokenCount
	}
	return got
}
