package caravan

import "github.com/kode4food/caravan/internal/stream/node"

// Take forwards only the first n messages
func (p StreamPipeline[Msg]) Take(n int) StreamPipeline[Msg] {
	return p.process(node.Take[Msg](n))
}

// TakeWhile forwards messages while the predicate is true
func (p StreamPipeline[Msg]) TakeWhile(
	fn func(Msg) bool,
) StreamPipeline[Msg] {
	return p.process(node.TakeWhile(fn))
}

// Skip drops the first n messages
func (p StreamPipeline[Msg]) Skip(n int) StreamPipeline[Msg] {
	return p.process(node.Skip[Msg](n))
}

// SkipWhile drops messages while the predicate is true
func (p StreamPipeline[Msg]) SkipWhile(
	fn func(Msg) bool,
) StreamPipeline[Msg] {
	return p.process(node.SkipWhile(fn))
}

// Distinct removes consecutive duplicate messages
func (p StreamPipeline[Msg]) Distinct(
	fn func(left, right Msg) bool,
) StreamPipeline[Msg] {
	return p.process(node.Distinct(fn))
}

// DistinctBy removes consecutive messages with duplicate keys
func (p StreamPipeline[Msg]) DistinctBy[Key comparable](
	fn func(Msg) Key,
) StreamPipeline[Msg] {
	return p.process(node.DistinctBy(fn))
}
