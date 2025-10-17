package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

func Generate[Msg any](
	fn Generator[Msg],
) stream.Processor[stream.Source, Msg] {
	return func(c *context.Context[stream.Source, Msg]) {
		for {
			if _, ok := c.FetchMessage(); !ok {
				return
			} else if res, ok := fn(); !ok {
				return
			} else if !c.ForwardResult(res) {
				return
			}
		}
	}
}

func GenerateFrom[Msg any](ch <-chan Msg) stream.Processor[stream.Source, Msg] {
	return Generate(func() (Msg, bool) {
		msg, ok := <-ch
		return msg, ok
	})
}
