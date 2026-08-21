package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Generate produces one message for each source request
func Generate[Msg any](
	fn func() (Msg, bool),
) stream.Processor[stream.Source, Msg] {
	return func(c *context.Context[stream.Source, Msg]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			}
			if res, ok := fn(); !ok || !c.ForwardResult(res) {
				return
			}
		}
	}
}

// GenerateFrom produces messages received from a channel
func GenerateFrom[Msg any](ch <-chan Msg) stream.Processor[stream.Source, Msg] {
	return Generate(func() (Msg, bool) {
		msg, ok := <-ch
		return msg, ok
	})
}
