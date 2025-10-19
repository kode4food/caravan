package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

func SidechainTo[Msg any](ch chan<- Msg) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {

		forwardToChannel := func(msg Msg) bool {
			select {
			case <-c.Done:
				return false
			case ch <- msg:
				return true
			}
		}

		for {
			msg, ok := c.FetchMessage()
			if !ok || !forwardToChannel(msg) || !c.ForwardResult(msg) {
				return
			}
		}
	}
}
