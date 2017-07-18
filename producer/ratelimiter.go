package producer

import (
	"sync"
	"time"
)

// TODO: add size rate limiting in addition to message count

// rateLimiter is used by the producer to rate limit the total number and size of records sent per cycle.
type rateLimiter struct {
	limit       int           // upper limit of throughput per cycle
	duration    time.Duration // frequency with which to reset the remaining tokens count
	tokenCount  int           // remaining tokens available for the cycle
	tokenMu     sync.Mutex    // mutex to protect remaining token count
	stopChannel chan empty    // channel for communicating when to stop rate limiting
	startOnce   sync.Once     // startOnce is used to ensure that start is called once and only once
	stopOnce    sync.Once     // stopOnce is used to ensure that stop is called once and only once
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
		ticker := time.NewTicker(r.duration)
		go func(){
			for {
				select {
				case <-r.stopChannel:
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

		// Reset startOnce to allow the rateLimiter to be started again
		r.startOnce = sync.Once{}
	})
}

// reset is called to reset the rateLimiter's tokens to the initial values
func (r *rateLimiter) reset() {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	r.tokenCount = r.limit
}

// getTokenCount is used to retrieve the current token count
func (r *rateLimiter) getTokenCount() int {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	return r.tokenCount
}

// claimTokens is used to claim tokens prior to sending messages
func (r *rateLimiter) claimTokens(count int) {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	r.tokenCount -= count
}
