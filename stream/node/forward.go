package node

import "github.com/kode4food/caravan/stream/context"

func Forward[Msg any](c *context.Context[Msg, Msg]) {
	for {
		msg, ok := c.FetchMessage()
		if !ok {
			return
		}

		if c.ForwardResult(msg) {
			continue
		}
		return
	}
}
