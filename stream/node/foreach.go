package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// ForEach constructs a processor that performs an action on the messages it
// sees using the provided function, and then forwards the message
func ForEach[Msg any](fn Consumer[Msg]) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			fn(msg)
			if !c.ForwardResult(msg) {
				return
			}
		}
	}
}
