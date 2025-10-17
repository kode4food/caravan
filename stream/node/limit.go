package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Take constructs a Processor that forwards only the first n messages
func Take[Msg any](n int) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		count := 0
		for count < n {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else {
				count++
				if !c.ForwardResult(msg) {
					return
				}
			}
		}
	}
}

// TakeWhile constructs a Processor that forwards messages until the
// predicate returns false
func TakeWhile[Msg any](pred Predicate[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if !pred(msg) {
				return
			} else if !c.ForwardResult(msg) {
				return
			}
		}
	}
}

// Skip constructs a Processor that skips the first n messages
func Skip[Msg any](n int) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		count := 0
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if count < n {
				count++
				continue
			} else if !c.ForwardResult(msg) {
				return
			}
		}
	}
}

// SkipWhile constructs a Processor that skips messages until the predicate
// returns false
func SkipWhile[Msg any](pred Predicate[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		skipping := true
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if skipping && pred(msg) {
				continue
			} else {
				skipping = false
				if !c.ForwardResult(msg) {
					return
				}
			}
		}
	}
}

// Distinct constructs a Processor that removes consecutive duplicate messages
// using the provided equality function
func Distinct[Msg any](eq Equality[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		var last *Msg

		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if last == nil || !eq(*last, msg) {
				msgCopy := msg
				last = &msgCopy
				if !c.ForwardResult(msg) {
					return
				}
			}
		}
	}
}

// DistinctBy constructs a Processor that removes consecutive duplicates based
// on a key function
func DistinctBy[Msg any, Key comparable](
	fn KeySelector[Msg, Key],
) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		var lastKey *Key

		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else {
				key := fn(msg)
				if lastKey == nil || *lastKey != key {
					lastKey = &key
					if !c.ForwardResult(msg) {
						return
					}
				}
			}
		}
	}
}
