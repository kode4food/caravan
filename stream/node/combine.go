package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Pair represents a paired result from two streams
type Pair[Left, Right any] struct {
	Left  Left
	Right Right
}

// Bind the output of the left Processor to the input of the right Processor,
// returning a new Processor that performs the handoff. If an error is reported
// by the left Processor, the handoff will be short-circuited and the error
// will be reported downstream.
//
// Processor[In, Out] = Processor[In, Bound] -> Processor[Bound, Out]
//
// In is the input type of the left Processor, Bound is the type of the left
// Processor's output as well as the input type of the right Processor. Out is
// the type of the right Processor's output
func Bind[In, Bound, Out any](
	left stream.Processor[In, Bound], right stream.Processor[Bound, Out],
) stream.Processor[In, Out] {
	return func(c *context.Context[In, Out]) {
		h := make(chan Bound)
		left.Start(context.WithOut(c, h))
		right.Start(context.WithIn(c, h))
	}
}

// Subprocess is the internal implementation of a Subprocess
func Subprocess[Msg any](
	p ...stream.Processor[Msg, Msg],
) stream.Processor[Msg, Msg] {
	switch len(p) {
	case 0:
		return Forward[Msg]
	case 1:
		return p[0]
	case 2:
		return Bind(p[0], p[1])
	default:
		return Bind(p[0], Subprocess[Msg](p[1:]...))
	}
}

// Merge forwards results from multiple Processors to the same channel
func Merge[Out any](
	p ...stream.Processor[stream.Source, Out],
) stream.Processor[stream.Source, Out] {
	return func(c *context.Context[stream.Source, Out]) {
		for _, proc := range p {
			proc.Start(c)
		}
	}
}

// Zip combines two streams by strictly pairing their elements in order.
// Emits pairs only when both streams have a message available
func Zip[Left, Right any](
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
) stream.Processor[stream.Source, Pair[Left, Right]] {
	return func(c *context.Context[stream.Source, Pair[Left, Right]]) {
		leftOut, rightOut := startBinaryProcessors(c, left, right)

		for {
			select {
			case <-c.Done:
				return
			case leftMsg, ok := <-leftOut:
				if !ok {
					return
				}
				select {
				case <-c.Done:
					return
				case rightMsg, ok := <-rightOut:
					if !ok {
						return
					}
					p := Pair[Left, Right]{Left: leftMsg, Right: rightMsg}
					if !c.ForwardResult(p) {
						return
					}
				}
			}
		}
	}
}

// ZipWith combines two streams using a custom combiner function
func ZipWith[Left, Right, Out any](
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
	combiner BinaryOperator[Left, Right, Out],
) stream.Processor[stream.Source, Out] {
	return func(c *context.Context[stream.Source, Out]) {
		leftOut, rightOut := startBinaryProcessors(c, left, right)

		for {
			select {
			case <-c.Done:
				return
			case leftMsg, ok := <-leftOut:
				if !ok {
					return
				}
				select {
				case <-c.Done:
					return
				case rightMsg, ok := <-rightOut:
					if !ok {
						return
					}
					if !c.ForwardResult(combiner(leftMsg, rightMsg)) {
						return
					}
				}
			}
		}
	}
}

// CombineLatest combines two streams by emitting whenever either stream
// produces a value, using the latest value from each stream
func CombineLatest[Left, Right, Out any](
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
	combiner BinaryOperator[Left, Right, Out],
) stream.Processor[stream.Source, Out] {
	return func(c *context.Context[stream.Source, Out]) {
		leftOut, rightOut := startBinaryProcessors(c, left, right)

		var (
			latestLeft  Left
			latestRight Right
			hasLeft     bool
			hasRight    bool
			leftClosed  bool
			rightClosed bool
		)

		for {
			if leftClosed && rightClosed {
				return
			}

			select {
			case <-c.Done:
				return
			case leftMsg, ok := <-leftOut:
				if !ok {
					leftClosed = true
					continue
				}
				latestLeft = leftMsg
				hasLeft = true
				if hasRight {
					if !c.ForwardResult(combiner(latestLeft, latestRight)) {
						return
					}
				}
			case rightMsg, ok := <-rightOut:
				if !ok {
					rightClosed = true
					continue
				}
				latestRight = rightMsg
				hasRight = true
				if hasLeft {
					if !c.ForwardResult(combiner(latestLeft, latestRight)) {
						return
					}
				}
			}
		}
	}
}

// Join accepts two Processors for the sake of joining their results based on a
// provided BinaryPredicate and BinaryOperator. If the predicate fails, nothing
// is forwarded, otherwise the two processed messages are combined using the
// join function, and the result is forwarded
func Join[Left, Right, Out any](
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
	pred BinaryPredicate[Left, Right],
	join BinaryOperator[Left, Right, Out],
) stream.Processor[stream.Source, Out] {
	return func(c *context.Context[stream.Source, Out]) {
		leftOut, rightOut := startBinaryProcessors(c, left, right)

		joinResults := func() (Left, Right, bool) {
			var leftZero Left
			var rightZero Right
			select {
			case <-c.Done:
				return leftZero, rightZero, false
			case leftMsg := <-leftOut:
				select {
				case <-c.Done:
					return leftZero, rightZero, false
				case rightMsg := <-rightOut:
					return leftMsg, rightMsg, true
				}
			case rightMsg := <-rightOut:
				select {
				case <-c.Done:
					return leftZero, rightZero, false
				case leftMsg := <-leftOut:
					return leftMsg, rightMsg, true
				}
			}
		}

		for {
			left, right, ok := joinResults()
			if !ok {
				return
			}
			if !pred(left, right) {
				continue
			}
			if !c.ForwardResult(join(left, right)) {
				return
			}
		}
	}
}

// startBinaryProcessors sets up two processors with their output channels
func startBinaryProcessors[Left, Right, Out any](
	c *context.Context[stream.Source, Out],
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
) (chan Left, chan Right) {
	leftOut := make(chan Left)
	rightOut := make(chan Right)
	left.Start(context.WithOut(c, leftOut))
	right.Start(context.WithOut(c, rightOut))
	return leftOut, rightOut
}
