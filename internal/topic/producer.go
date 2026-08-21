package topic

import (
	"errors"
	"log/slog"
	"runtime"

	"github.com/kode4food/caravan/closer"
)

type producer[Msg any] struct {
	closer.Closer
	topic   *topicState[Msg]
	channel chan Msg
	id      uint64
}

var (
	errProducerNotClosed = errors.New("producer not closed")
)

func newProducer[Msg any](t *topicState[Msg]) *producer[Msg] {
	ch := startProducer(t)
	res := &producer[Msg]{
		id:      t.nextID.Add(1),
		topic:   t,
		channel: ch,
		Closer: newCloser(func() {
			close(ch)
		}),
	}
	runtime.SetFinalizer(res, producerDebugFinalizer[Msg])
	return res
}

// Send returns the Producer's message channel
func (p *producer[Msg]) Send() chan<- Msg {
	return p.channel
}

func startProducer[Msg any](t *topicState[Msg]) chan Msg {
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
		slog.Debug(errProducerNotClosed.Error(),
			slog.Uint64("producer_id", p.id))
	}
}
