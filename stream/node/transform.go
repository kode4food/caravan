package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Filter constructs a Processor that will only forward its messages if the
// provided function returns true
func Filter[Msg any](fn Predicate[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			if !fn(msg) {
				continue
			}
			if c.ForwardResult(msg) {
				continue
			}
			return
		}
	}
}

// Map constructs a processor that maps the messages it sees into new messages
// using the provided function
func Map[From, To any](fn Mapper[From, To]) stream.Processor[From, To] {
	return func(c *context.Context[From, To]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			if c.ForwardResult(fn(msg)) {
				continue
			}
			return
		}
	}
}

// FlatMap constructs a Processor that maps each message to zero or more
// messages and forwards them all downstream
func FlatMap[From, To any](fn FlatMapper[From, To]) stream.Processor[From, To] {
	return func(c *context.Context[From, To]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			results := fn(msg)
			allForwarded := true
			for _, result := range results {
				if !c.ForwardResult(result) {
					allForwarded = false
					break
				}
			}
			if allForwarded {
				continue
			}
			return
		}
	}
}
