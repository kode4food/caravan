package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

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

// ZipWith combines two streams using a custom combiner function
func ZipWith[Left, Right, Out any](
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
	combiner func(Left, Right) Out,
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
					if !ok || !c.ForwardResult(combiner(leftMsg, rightMsg)) {
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
	combiner func(Left, Right) Out,
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

// Join combines paired messages that satisfy a predicate
func Join[Left, Right, Out any](
	left stream.Processor[stream.Source, Left],
	right stream.Processor[stream.Source, Right],
	pred func(Left, Right) bool,
	join func(Left, Right) Out,
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
			if !ok || pred(left, right) && !c.ForwardResult(join(left, right)) {
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
