package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

func Sink[Msg any]() stream.Processor[Msg, stream.Sink] {
	return func(c *context.Context[Msg, stream.Sink]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			}
		}
	}
}

func SinkInto[Msg any](ch chan<- Msg) stream.Processor[Msg, stream.Sink] {
	return Bind(
		SidechainTo(ch),
		Sink[Msg](),
	)
}
