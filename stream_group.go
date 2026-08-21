package caravan

import (
	_stream "github.com/kode4food/caravan/internal/stream"
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/topic"
)

type (
	// GroupedPipeline builds the remaining stages of a grouped Stream
	GroupedPipeline[Msg any, Key comparable] struct {
		source stream.Processor[stream.Source, *Grouped[Msg, Key]]
	}

	// Grouped contains a message and its selected grouping key
	Grouped[Msg any, Key comparable] struct {
		Message Msg
		Key     Key
	}
)

// GroupBy groups messages using the provided selector
func (p StreamPipeline[Msg]) GroupBy[Key comparable](
	fn func(Msg) Key,
) GroupedPipeline[Msg, Key] {
	group := node.Map(func(msg Msg) *Grouped[Msg, Key] {
		return &Grouped[Msg, Key]{Message: msg, Key: fn(msg)}
	})
	return GroupedPipeline[Msg, Key]{
		source: _stream.Bind(p.source, group),
	}
}

// Build completes a grouped Stream
func (g GroupedPipeline[Msg, Key]) Build() stream.Stream {
	return _stream.New(g.source)
}

// Map transforms grouped messages
func (g GroupedPipeline[Msg, Key]) Map[To any](
	fn func(*Grouped[Msg, Key]) To,
) StreamPipeline[To] {
	return StreamPipeline[To]{
		source: _stream.Bind(g.source, node.Map(fn)),
	}
}

// ScanFrom emits grouped aggregates from an initial value
func (g GroupedPipeline[Msg, Key]) ScanFrom[Out any](
	fn func(Out, *Grouped[Msg, Key]) Out, init Out,
) StreamPipeline[Out] {
	return StreamPipeline[Out]{
		source: _stream.Bind(g.source, node.ScanFrom(fn, init)),
	}
}

// TopicProducer completes a grouped Stream by sending messages to a Topic
func (g GroupedPipeline[Msg, Key]) TopicProducer(
	t topic.Topic[*Grouped[Msg, Key]],
) stream.Stream {
	return _stream.New(
		_stream.Bind(g.source, node.TopicProducer(t)),
	)
}
