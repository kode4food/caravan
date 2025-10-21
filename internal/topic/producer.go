package topic

import (
	"fmt"
	"runtime"

	"github.com/google/uuid"

	"github.com/kode4food/caravan/closer"
	"github.com/kode4food/caravan/topic"
)

type producer[Msg any] struct {
	closer.Closer
	id      uuid.UUID
	topic   *Topic[Msg]
	channel chan Msg
}

func makeProducer[Msg any](t *Topic[Msg]) *producer[Msg] {
	ch := startProducer(t)
	res := &producer[Msg]{
		id:      uuid.New(),
		topic:   t,
		channel: ch,
		Closer: makeCloser(func() {
			close(ch)
		}),
	}

	if Debug.IsEnabled() {
		wrap := WrapStackTrace(MsgInstantiationTrace)
		runtime.SetFinalizer(res, producerDebugFinalizer[Msg](wrap))
	}
	return res
}

func (p *producer[Msg]) Send() chan<- Msg {
	return p.channel
}

func startProducer[Msg any](t *Topic[Msg]) chan Msg {
	ch := make(chan Msg)
	go func() {
		defer func() {
			// probably because the channel was closed
			recover()
		}()
		for e := range ch {
			t.put(e)
		}
	}()
	return ch
}

func producerDebugFinalizer[Msg any](
	wrap ErrorWrapper,
) func(*producer[Msg]) {
	return func(p *producer[Msg]) {
		select {
		case <-p.IsClosed():
			// already closed, nothing to do
		default:
			// not closed, report error
			Debug.WithProducer(func(dp topic.Producer[error]) {
				err := fmt.Errorf(topic.ErrProducerNotClosed, p.id)
				dp.Send() <- wrap(err)
			})
		}
	}
}
