package node

import (
	"time"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Throttle constructs a Processor that limits message emission to at most
// one message per specified duration
func Throttle[Msg any](rate time.Duration) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		ticker := time.NewTicker(rate)
		defer ticker.Stop()

		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			<-ticker.C
			if c.ForwardResult(msg) {
				continue
			}
			return
		}
	}
}

// Debounce constructs a Processor that emits a message only after the
// specified duration has passed with no new messages arriving
func Debounce[Msg any](d time.Duration) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		var timer *time.Timer
		pending := make(chan Msg, 1)
		done := make(chan struct{})
		defer close(done)

		go func() {
			for {
				select {
				case <-done:
					return
				case msg := <-pending:
					if timer != nil {
						timer.Stop()
					}
					timer = time.AfterFunc(d, func() {
						c.ForwardResult(msg)
					})
				}
			}
		}()

		for {
			msg, ok := c.FetchMessage()
			if !ok {
				if timer != nil {
					timer.Stop()
				}
				return
			}

			select {
			case pending <- msg:
			default:
				<-pending
				pending <- msg
			}
		}
	}
}

// Delay constructs a Processor that delays each message by a fixed duration
// before forwarding it downstream
func Delay[Msg any](d time.Duration) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			time.Sleep(d)
			if c.ForwardResult(msg) {
				continue
			}
			return
		}
	}
}

// Sample emits at most one message per time period, dropping messages that
// arrive while waiting for the next sample window
func Sample[Msg any](period time.Duration) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		ticker := time.NewTicker(period)
		defer ticker.Stop()

		var pending *Msg
		hasPending := false

		for {
			select {
			case <-c.Done:
				return
			case <-ticker.C:
				if hasPending {
					if !c.ForwardResult(*pending) {
						return
					}
					hasPending = false
					pending = nil
				}
			default:
				if msg, ok := c.FetchMessage(); ok {
					// Store latest message, will be emitted on next tick
					pending = &msg
					hasPending = true
					continue
				}
				// Flush any pending message before closing
				if hasPending {
					c.ForwardResult(*pending)
				}
				return
			}
		}
	}
}
