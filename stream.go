package caravan

import (
	_stream "github.com/kode4food/caravan/internal/stream"
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/topic"
)

// StreamPipeline builds the remaining stages of a Stream
type StreamPipeline[Msg any] struct {
	source stream.Processor[stream.Source, Msg]
}

// Build completes a Stream
func (p StreamPipeline[Msg]) Build() stream.Stream {
	return _stream.New(p.source)
}

// TopicProducer completes a Stream by sending messages to a Topic
func (p StreamPipeline[Msg]) TopicProducer(t topic.Topic[Msg]) stream.Stream {
	return p.process(node.TopicProducer(t)).Build()
}

// SinkInto completes a Stream by sending messages to a channel
func (p StreamPipeline[Msg]) SinkInto(ch chan<- Msg) stream.Stream {
	return p.process(node.SinkInto(ch)).Build()
}

// TopicConsumer starts a Stream with messages from a Topic
func TopicConsumer[Msg any](t topic.Topic[Msg]) StreamPipeline[Msg] {
	return start(node.TopicConsumer(t))
}

// Generate starts a Stream with messages from a generator
func Generate[Msg any](fn func() (Msg, bool)) StreamPipeline[Msg] {
	return start(node.Generate(fn))
}

// GenerateFrom starts a Stream with messages from a channel
func GenerateFrom[Msg any](ch <-chan Msg) StreamPipeline[Msg] {
	return start(node.GenerateFrom(ch))
}

func (p StreamPipeline[From]) process[To any](
	processor stream.Processor[From, To],
) StreamPipeline[To] {
	return StreamPipeline[To]{
		source: _stream.Bind(p.source, processor),
	}
}

func start[Msg any](
	processor stream.Processor[stream.Source, Msg],
) StreamPipeline[Msg] {
	return StreamPipeline[Msg]{source: processor}
}
