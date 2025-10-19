package node

import (
	"time"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Window constructs a Processor that collects messages into time-based
// windows. Emits a batch every duration, regardless of size
func Window[Msg any](duration time.Duration) stream.Processor[Msg, []Msg] {
	return func(c *context.Context[Msg, []Msg]) {
		ticker := time.NewTicker(duration)
		defer ticker.Stop()

		window := make([]Msg, 0)

		for {
			select {
			case <-ticker.C:
				if len(window) > 0 {
					if !c.ForwardResult(window) {
						return
					}
					window = make([]Msg, 0)
				}
			default:
				if msg, ok := c.FetchMessage(); ok {
					window = append(window, msg)
					continue
				}
				// Flush remaining window on close
				if len(window) > 0 {
					c.ForwardResult(window)
				}
				return
			}
		}
	}
}

// SlidingWindow constructs a Processor that collects messages into sliding
// windows of a fixed size. Emits a new window after each message
func SlidingWindow[Msg any](size int) stream.Processor[Msg, []Msg] {
	return func(c *context.Context[Msg, []Msg]) {
		window := make([]Msg, 0, size)

		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			window = append(window, msg)
			if len(window) > size {
				window = window[1:]
			}
			if len(window) == size && !c.ForwardResult(window) {
				return
			}
		}
	}
}
