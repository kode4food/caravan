package node

import (
	"time"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

const backoffMultiplier = 2

// Retry constructs a Processor that retries a mapping function on failure
// with exponential backoff. Drops messages that fail after max attempts
func Retry[From, To any](
	fn func(From) (To, error), maxAttempts int,
	initialBackoff time.Duration,
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

			for attempt := range maxAttempts {
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
