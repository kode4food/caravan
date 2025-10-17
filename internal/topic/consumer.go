package topic

import (
	"fmt"
	"runtime"
	"time"

	"github.com/google/uuid"

	"github.com/kode4food/caravan/closer"
	"github.com/kode4food/caravan/topic"
	"github.com/kode4food/caravan/topic/backoff"
)

type consumer[Msg any] struct {
	*cursor[Msg]
	id      uuid.UUID
	channel chan Msg
}

func makeConsumer[Msg any](c *cursor[Msg], b backoff.Generator) *consumer[Msg] {
	res := &consumer[Msg]{
		cursor:  c,
		id:      c.id,
		channel: startConsumer(c, b),
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

func startConsumer[Msg any](c *cursor[Msg], b backoff.Generator) chan Msg {
	ch := make(chan Msg)
	next := b()
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
					case <-time.After(next()):
						// allow retention policies to kick in while waiting
						// for a channel read to happen
					case ch <- e:
						// advance the cursor and reset the backoff sequence
						c.advance()
						next = b()
					}
				} else {
					// Wait for something to happen
					select {
					case <-c.IsClosed():
						goto closed
					case <-time.After(next()):
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
		if !closer.IsClosed(c) {
			Debug.WithProducer(func(dp topic.Producer[error]) {
				err := fmt.Errorf(topic.ErrConsumerNotClosed, c.id)
				dp.Send() <- wrap(err)
			})
		}
	}
}
