package task

import "time"

// backoff returns the delay before the next attempt: exponential from base,
// capped at maxDelay, with jitter in [d/2, d]. jitter must return a value in [0, 1).
func backoff(attempt int, base, maxDelay time.Duration, jitter func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt && delay < maxDelay; i++ {
		delay *= 2
	}
	delay = min(delay, maxDelay)
	return delay/2 + time.Duration(jitter()*float64(delay/2))
}
