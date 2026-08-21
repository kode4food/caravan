package caravan

import "github.com/kode4food/caravan/internal/stream/node"

// Reduce reduces messages into an aggregate
func (p StreamPipeline[In]) Reduce[Out any](
	fn func(Out, In) Out,
) StreamPipeline[Out] {
	return p.process(node.Reduce(fn))
}

// ReduceFrom reduces messages from an initial value
func (p StreamPipeline[In]) ReduceFrom[Out any](
	fn func(Out, In) Out, init Out,
) StreamPipeline[Out] {
	return p.process(node.ReduceFrom(fn, init))
}

// Scan emits each intermediate aggregate
func (p StreamPipeline[In]) Scan[Out any](
	fn func(Out, In) Out,
) StreamPipeline[Out] {
	return p.process(node.Scan(fn))
}

// ScanFrom emits intermediate aggregates from an initial value
func (p StreamPipeline[In]) ScanFrom[Out any](
	fn func(Out, In) Out, init Out,
) StreamPipeline[Out] {
	return p.process(node.ScanFrom(fn, init))
}
