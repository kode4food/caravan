package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

type initialReduction[Res any] func() Res

// Reduce constructs a processor that reduces the messages it sees into some
// form of aggregated messages, based on the provided function
func Reduce[In, Out any](r Reducer[Out, In]) stream.Processor[In, Out] {
	return reduce(r, nil)
}

// ReduceFrom constructs a processor that reduces the messages it sees into
// some form of aggregated messages, based on the provided function and an
// initial message
func ReduceFrom[In, Out any](
	r Reducer[Out, In], init Out,
) stream.Processor[In, Out] {
	return reduce(r, func() Out {
		return init
	})
}

func reduce[In, Out any](
	fn Reducer[Out, In], initial initialReduction[Out],
) stream.Processor[In, Out] {
	var fetchFirst func(*context.Context[In, Out]) (Out, bool)

	if initial != nil {
		fetchFirst = func(_ *context.Context[In, Out]) (Out, bool) {
			return initial(), true
		}
	} else {
		fetchFirst = func(c *context.Context[In, Out]) (Out, bool) {
			var zero Out
			if msg, ok := c.FetchMessage(); ok {
				return fn(zero, msg), true
			}
			return zero, false
		}
	}

	return func(c *context.Context[In, Out]) {
		res, ok := fetchFirst(c)
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

// Scan constructs a Processor that applies a reducer function to each message
// and emits all intermediate results. Unlike Reduce which only emits the final
// result, Scan emits after each message
func Scan[In, Out any](r Reducer[Out, In]) stream.Processor[In, Out] {
	return scan(r, nil)
}

// ScanFrom constructs a Processor that applies a reducer function to each
// message starting from an initial value, emitting all intermediate results
func ScanFrom[In, Out any](
	r Reducer[Out, In], init Out,
) stream.Processor[In, Out] {
	return scan(r, func() Out {
		return init
	})
}

func scan[In, Out any](
	fn Reducer[Out, In], initial initialReduction[Out],
) stream.Processor[In, Out] {
	var res Out

	if initial != nil {
		res = initial()
	} else {
		// For Scan without initial value, use zero value as starting point
		var zero Out
		res = zero
	}

	return func(c *context.Context[In, Out]) {
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
