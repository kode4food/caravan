package node

import (
	"time"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Buffer constructs a Processor that collects messages into batches based on
// size or time. Emits a batch when either the size limit is reached or maxWait
// duration passes
func Buffer[Msg any](
	size int, maxWait time.Duration,
) stream.Processor[Msg, []Msg] {
	return func(c *context.Context[Msg, []Msg]) {
		batch := make([]Msg, 0, size)
		timer := time.NewTimer(maxWait)
		defer timer.Stop()

		flush := func() bool {
			if len(batch) > 0 {
				if !c.ForwardResult(batch) {
					return false
				}
				batch = make([]Msg, 0, size)
			}
			timer.Reset(maxWait)
			return true
		}

		for {
			msg, ok := c.FetchMessage()
			if !ok {
				flush()
				return
			}

			batch = append(batch, msg)
			if len(batch) >= size && !flush() {
				return
			}
		}
	}
}
