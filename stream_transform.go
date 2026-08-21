package caravan

import "github.com/kode4food/caravan/internal/stream/node"

// Filter forwards messages accepted by the predicate
func (p StreamPipeline[Msg]) Filter(
	fn func(Msg) bool,
) StreamPipeline[Msg] {
	return p.process(node.Filter(fn))
}

// Map transforms each message
func (p StreamPipeline[From]) Map[To any](
	fn func(From) To,
) StreamPipeline[To] {
	return p.process(node.Map(fn))
}

// FlatMap transforms each message into zero or more messages
func (p StreamPipeline[From]) FlatMap[To any](
	fn func(From) []To,
) StreamPipeline[To] {
	return p.process(node.FlatMap(fn))
}

// ForEach performs an action for each message
func (p StreamPipeline[Msg]) ForEach(fn func(Msg)) StreamPipeline[Msg] {
	return p.process(node.ForEach(fn))
}
