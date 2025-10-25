package topic

import (
	"errors"
	"log/slog"
	"runtime"

	"github.com/google/uuid"

	"github.com/kode4food/caravan/closer"
)

type producer[Msg any] struct {
	closer.Closer
	topic   *Topic[Msg]
	channel chan Msg
	id      uuid.UUID
}

var (
	ErrProducerNotClosed = errors.New("producer not closed")
)

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
	runtime.SetFinalizer(res, producerDebugFinalizer[Msg])
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

func producerDebugFinalizer[Msg any](p *producer[Msg]) {
	select {
	case <-p.IsClosed():
	default:
		slog.Debug(ErrProducerNotClosed.Error(),
			slog.String("producer_id", p.id.String()),
		)
	}
}
