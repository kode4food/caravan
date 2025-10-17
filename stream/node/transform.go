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
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if !fn(msg) {
				continue
			} else if !c.ForwardResult(msg) {
				return
			}
		}
	}
}

// Map constructs a processor that maps the messages it sees into new messages
// using the provided function
func Map[From, To any](fn Mapper[From, To]) stream.Processor[From, To] {
	return func(c *context.Context[From, To]) {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if !c.ForwardResult(fn(msg)) {
				return
			}
		}
	}
}

// FlatMap constructs a Processor that maps each message to zero or more
// messages and forwards them all downstream
func FlatMap[From, To any](fn FlatMapper[From, To]) stream.Processor[From, To] {
	return func(c *context.Context[From, To]) {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else {
				results := fn(msg)
				for _, result := range results {
					if !c.ForwardResult(result) {
						return
					}
				}
			}
		}
	}
}
