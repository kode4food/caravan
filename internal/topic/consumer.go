package topic

import (
	"fmt"
	"runtime"

	"github.com/google/uuid"

	"github.com/kode4food/caravan/topic"
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

	if Debug.IsEnabled() {
		wrap := WrapStackTrace(MsgInstantiationTrace)
		runtime.SetFinalizer(res, consumerDebugFinalizer[Msg](wrap))
	}
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

func consumerDebugFinalizer[Msg any](
	wrap ErrorWrapper,
) func(c *consumer[Msg]) {
	return func(c *consumer[Msg]) {
		select {
		case <-c.IsClosed():
		default:
			Debug.WithProducer(func(dp topic.Producer[error]) {
				err := fmt.Errorf(topic.ErrConsumerNotClosed, c.id)
				dp.Send() <- wrap(err)
			})
		}
	}
}
