package topic

import (
	"errors"
	"log/slog"
	"runtime"
)

type consumer[Msg any] struct {
	*cursor[Msg]
	channel chan Msg
	id      uint64
}

var (
	errConsumerNotClosed = errors.New("consumer not closed")
)

func newConsumer[Msg any](c *cursor[Msg]) *consumer[Msg] {
	res := &consumer[Msg]{
		cursor:  c,
		id:      c.id,
		channel: startConsumer(c),
	}
	runtime.SetFinalizer(res, consumerDebugFinalizer[Msg])
	return res
}

// Receive returns the Consumer's message channel
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
						c.offset.Add(1)
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
		slog.Debug(errConsumerNotClosed.Error(),
			slog.Uint64("consumer_id", c.id))
	}
}
