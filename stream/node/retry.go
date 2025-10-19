package node

import (
	"time"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

const backoffMultiplier = 2

// RetryMapper is a function that can fail and should be retried
type RetryMapper[From, To any] func(From) (To, error)

// Retry constructs a Processor that retries a mapping function on failure
// with exponential backoff. Drops messages that fail after max attempts
func Retry[From, To any](
	fn RetryMapper[From, To], maxAttempts int, initialBackoff time.Duration,
) stream.Processor[From, To] {
	return func(c *context.Context[From, To]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			backoff := initialBackoff
			var result To
			var err error

			for attempt := 0; attempt < maxAttempts; attempt++ {
				result, err = fn(msg)
				if err == nil {
					break
				}
				if attempt < maxAttempts-1 {
					time.Sleep(backoff)
					backoff *= backoffMultiplier
				}
			}

			// Only forward if we succeeded
			if err == nil && !c.ForwardResult(result) {
				return
			}
		}
	}
}
