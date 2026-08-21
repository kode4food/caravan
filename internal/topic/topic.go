package topic

import (
	"sync"
	"sync/atomic"

	"github.com/kode4food/caravan/closer"
	"github.com/kode4food/caravan/internal/sync/channel"
	"github.com/kode4food/caravan/topic"
)

type (
	topicState[Msg any] struct {
		closer.Closer
		log         *log[Msg]
		cursors     *cursors[Msg]
		observers   *topicObservers
		vacuumReady *channel.ReadyWait
		nextID      atomic.Uint64
	}

	// topicObservers manages a set of callbacks for observers of a Topic
	topicObservers struct {
		callbacks map[uint64]func()
		mu        sync.RWMutex
	}
)

const defaultSegmentSize = 256

// New returns an empty Topic
func New[Msg any]() topic.Topic[Msg] {
	t := &topicState[Msg]{
		cursors: &cursors[Msg]{
			cursors: map[uint64]*cursor[Msg]{},
		},
		observers: &topicObservers{
			callbacks: map[uint64]func(){},
		},
		log: newLog[Msg](defaultSegmentSize),
	}
	t.Closer = newCloser(func() {
		if t.vacuumReady != nil {
			t.vacuumReady.Close()
		}
	})
	t.startVacuuming()
	return t
}

// Length returns the virtual size of the Topic
func (t *topicState[_]) Length() uint64 {
	return t.log.length()
}

// NewProducer instantiates a new Topic Producer
func (t *topicState[Msg]) NewProducer() topic.Producer[Msg] {
	return newProducer(t)
}

// NewConsumer instantiates a new Topic Consumer
func (t *topicState[Msg]) NewConsumer() topic.Consumer[Msg] {
	return newConsumer(t.newCursor())
}

// get consumes a message starting at the specified virtual Offset within the
// Topic. If the offset is no longer being retained, the next available offset
// will be consumed. The actual offset read is returned
func (t *topicState[Msg]) get(o uint64) (Msg, uint64, bool) {
	defer t.vacuumReady.Notify()
	return t.log.get(o)
}

// put adds the specified Message to the Topic
func (t *topicState[Msg]) put(msg Msg) {
	t.log.put(msg)
	t.observers.notify()
}

func (t *topicState[_]) startVacuuming() {
	vacuumID := t.nextID.Add(1)
	ready := channel.NewReadyWait()
	t.vacuumReady = ready
	t.observers.add(vacuumID, ready.Notify)

	go func() {
		for {
			select {
			case <-t.IsClosed():
				return
			case <-ready.Wait():
				if t.log.canVacuum() {
					t.vacuum()
				}
			}
		}
	}()
}

func (t *topicState[Msg]) vacuum() {
	offsets := t.cursors.offsets()
	t.log.vacuum(func(e *segment[Msg]) bool {
		start := t.log.start()
		lastOffset := start + uint64(e.length()-1)

		for _, o := range offsets {
			if o <= lastOffset {
				return true
			}
		}
		return false
	})
}

func (t *topicState[Msg]) newCursor() *cursor[Msg] {
	c := newCursor(t)
	t.cursors.track(c)
	t.observers.add(c.id, c.ready.Notify)
	return c
}

func (o *topicObservers) add(i uint64, cb func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.callbacks[i] = cb
}

func (o *topicObservers) remove(i uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.callbacks, i)
}

func (o *topicObservers) notify() {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, cb := range o.callbacks {
		cb()
	}
}
