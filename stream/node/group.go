package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

type (
	Grouped[Msg any, Key comparable] struct {
		msg Msg
		key Key
	}

	GroupSelector[Msg any, Key comparable] func(Msg) Key
)

func GroupBy[Msg any, Key comparable](
	fn GroupSelector[Msg, Key],
) stream.Processor[Msg, *Grouped[Msg, Key]] {
	return func(c *context.Context[Msg, *Grouped[Msg, Key]]) {
		for {
			if msg, ok := c.FetchMessage(); !ok {
				return
			} else if !c.ForwardResult(&Grouped[Msg, Key]{
				key: fn(msg),
				msg: msg,
			}) {
				return
			}
		}
	}
}

func (g *Grouped[_, Key]) Key() Key {
	return g.key
}

func (g *Grouped[Msg, _]) Message() Msg {
	return g.msg
}

func GroupedKey[Msg any, Key comparable](m *Grouped[Msg, Key]) Key {
	return m.Key()
}

func GroupedMessage[Msg any, Key comparable](m *Grouped[Msg, Key]) Msg {
	return m.Message()
}
