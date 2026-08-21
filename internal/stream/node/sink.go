package node

import (
	_stream "github.com/kode4food/caravan/internal/stream"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Sink consumes and discards every input message
func Sink[Msg any]() stream.Processor[Msg, stream.Sink] {
	return func(c *context.Context[Msg, stream.Sink]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			}
		}
	}
}

// SinkInto consumes messages by sending them to a channel
func SinkInto[Msg any](ch chan<- Msg) stream.Processor[Msg, stream.Sink] {
	return _stream.Bind(
		SidechainTo(ch),
		Sink[Msg](),
	)
}
