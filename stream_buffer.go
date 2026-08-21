package caravan

import (
	"time"

	_stream "github.com/kode4food/caravan/internal/stream"
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/table"
	"github.com/kode4food/caravan/topic"
)

// BatchPipeline builds the remaining stages of a batched Stream
type BatchPipeline[Msg any] struct {
	source stream.Processor[stream.Source, []Msg]
}

// Buffer collects messages into batches
func (p StreamPipeline[Msg]) Buffer(
	size int, maxWait time.Duration,
) BatchPipeline[Msg] {
	return BatchPipeline[Msg]{
		source: _stream.Bind(p.source, node.Buffer[Msg](size, maxWait)),
	}
}

// Window collects messages into time-based windows
func (p StreamPipeline[Msg]) Window(d time.Duration) BatchPipeline[Msg] {
	return BatchPipeline[Msg]{
		source: _stream.Bind(p.source, node.Window[Msg](d)),
	}
}

// SlidingWindow collects messages into fixed-size sliding windows
func (p StreamPipeline[Msg]) SlidingWindow(size int) BatchPipeline[Msg] {
	return BatchPipeline[Msg]{
		source: _stream.Bind(p.source, node.SlidingWindow[Msg](size)),
	}
}

// Build completes a batched Stream
func (b BatchPipeline[Msg]) Build() stream.Stream {
	return _stream.New(b.source)
}

// FlatMap transforms each batch into zero or more messages
func (b BatchPipeline[From]) FlatMap[To any](
	fn func([]From) []To,
) StreamPipeline[To] {
	return StreamPipeline[To]{
		source: _stream.Bind(b.source, node.FlatMap(fn)),
	}
}

// TableBatchUpdate updates a Table for each message in a batch
func (b BatchPipeline[Msg]) TableBatchUpdate[Key comparable, Value any](
	updater table.Updater[Msg, Key, Value],
) BatchPipeline[Msg] {
	return BatchPipeline[Msg]{
		source: _stream.Bind(b.source, node.TableBatchUpdate(updater)),
	}
}

// TopicProducer completes a batched Stream by sending batches to a Topic
func (b BatchPipeline[Msg]) TopicProducer(
	t topic.Topic[[]Msg],
) stream.Stream {
	return _stream.New(
		_stream.Bind(b.source, node.TopicProducer(t)),
	)
}
