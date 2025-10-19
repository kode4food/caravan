package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Take constructs a Processor that forwards only the first n messages
func Take[Msg any](n int) stream.Processor[Msg, Msg] {
	count := 0
	return TakeWhile(func(_ Msg) bool {
		if count < n {
			count++
			return true
		}
		return false
	})
}

// TakeWhile constructs a Processor that forwards messages until the
// predicate returns false
func TakeWhile[Msg any](pred Predicate[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			if !pred(msg) || !c.ForwardResult(msg) {
				return
			}
		}
	}
}

// Skip constructs a Processor that skips the first n messages
func Skip[Msg any](n int) stream.Processor[Msg, Msg] {
	count := 0
	return SkipWhile(func(_ Msg) bool {
		if count < n {
			count++
			return true
		}
		return false
	})
}

// SkipWhile constructs a Processor that skips messages until the predicate
// returns false
func SkipWhile[Msg any](pred Predicate[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		skipping := true
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			if skipping && pred(msg) {
				continue
			}
			skipping = false

			if !c.ForwardResult(msg) {
				return
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
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			if last != nil && eq(*last, msg) {
				continue
			}
			msgCopy := msg
			last = &msgCopy

			if !c.ForwardResult(msg) {
				return
			}
		}
	}
}

// DistinctBy constructs a Processor that removes consecutive duplicates based
// on a key function
func DistinctBy[Msg any, Key comparable](
	fn KeySelector[Msg, Key],
) stream.Processor[Msg, Msg] {
	return Distinct(func(a, b Msg) bool {
		return fn(a) == fn(b)
	})
}
