package topic

import (
	"log/slog"
	"runtime"

	"github.com/google/uuid"
)

type consumer[Msg any] struct {
	*cursor[Msg]
	id      uuid.UUID
	channel chan Msg
}

func makeConsumer[Msg any](c *cursor[Msg]) *consumer[Msg] {
	res := &consumer[Msg]{
		cursor:  c,
		id:      c.id,
		channel: startConsumer(c),
	}
	runtime.SetFinalizer(res, consumerDebugFinalizer[Msg])
	return res
}

func (c *consumer[Msg]) Receive() <-chan Msg {
	return c.channel
}

func startConsumer[Msg any](c *cursor[Msg]) chan Msg {
	ch := make(chan Msg)
	go func() {
		defer func() {
			// probably because the channel was closed
			recover()
		}()
		for {
			select {
			case <-c.IsClosed():
				goto closed
			default:
				if e, ok := c.head(); ok {
					select {
					case <-c.IsClosed():
						goto closed
					case ch <- e:
						c.advance()
					}
				} else {
					// Wait for something to happen
					select {
					case <-c.IsClosed():
						goto closed
					case <-c.ready.Wait():
					}
				}
			}
		}
	closed:
		close(ch)
	}()
	return ch
}

func consumerDebugFinalizer[Msg any](c *consumer[Msg]) {
	select {
	case <-c.IsClosed():
	default:
		slog.Debug("consumer not closed before garbage collection", "id", c.id)
	}
}
