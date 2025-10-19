package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Reduce constructs a processor that reduces the messages it sees into some
// form of aggregated messages, based on the provided function
func Reduce[In, Out any](r Reducer[Out, In]) stream.Processor[In, Out] {
	return reduce(r, func(c *context.Context[In, Out]) (Out, bool) {
		var zero Out
		if msg, ok := c.FetchMessage(); ok {
			return r(zero, msg), true
		}
		return zero, false
	})
}

// ReduceFrom constructs a processor that reduces the messages it sees into
// some form of aggregated messages, based on the provided function and an
// initial message
func ReduceFrom[In, Out any](
	r Reducer[Out, In], init Out,
) stream.Processor[In, Out] {
	return reduce(r, func(_ *context.Context[In, Out]) (Out, bool) {
		return init, true
	})
}

// Scan constructs a Processor that applies a reducer function to each message
// and emits all intermediate results. Unlike Reduce which only emits the final
// result, Scan emits after each message
func Scan[In, Out any](r Reducer[Out, In]) stream.Processor[In, Out] {
	var zero Out
	return reduce(r, func(_ *context.Context[In, Out]) (Out, bool) {
		return zero, true
	})
}

// ScanFrom constructs a Processor that applies a reducer function to each
// message starting from an initial value, emitting all intermediate results
func ScanFrom[In, Out any](
	r Reducer[Out, In], init Out,
) stream.Processor[In, Out] {
	return reduce(r, func(_ *context.Context[In, Out]) (Out, bool) {
		return init, true
	})
}

// reduce provides the common reduction loop for both reduce and scan
func reduce[In, Out any](
	fn Reducer[Out, In], init func(*context.Context[In, Out]) (Out, bool),
) stream.Processor[In, Out] {
	return func(c *context.Context[In, Out]) {
		res, ok := init(c)
		if !ok {
			return
		}
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			res = fn(res, msg)
			if !c.ForwardResult(res) {
				return
			}
		}
	}
}
