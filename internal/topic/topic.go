package topic

import (
	"sync"

	"github.com/google/uuid"

	"github.com/kode4food/caravan/closer"
	"github.com/kode4food/caravan/internal/sync/channel"
	"github.com/kode4food/caravan/topic"
)

type (
	// Topic is the internal implementation of a Topic
	Topic[Msg any] struct {
		closer.Closer
		log         *Log[Msg]
		cursors     *cursors[Msg]
		observers   *topicObservers
		vacuumReady *channel.ReadyWait
	}

	// topicObservers manages a set of callbacks for observers of a Topic
	topicObservers struct {
		callbacks map[uuid.UUID]func()
		mu        sync.RWMutex
	}
)

const defaultSegmentSize = 256

// Make instantiates a new internal Topic instance
func Make[Msg any]() topic.Topic[Msg] {
	t := &Topic[Msg]{
		cursors:   makeCursors[Msg](),
		observers: makeLogObservers(),
		log:       makeLog[Msg](defaultSegmentSize),
	}
	t.Closer = makeCloser(func() {
		if t.vacuumReady != nil {
			t.vacuumReady.Close()
		}
	})
	t.startVacuuming()
	return t
}

// Length returns the virtual size of the Topic
func (t *Topic[_]) Length() uint64 {
	return t.log.length()
}

// NewProducer instantiates a new Topic Producer
func (t *Topic[Msg]) NewProducer() topic.Producer[Msg] {
	return makeProducer(t)
}

// NewConsumer instantiates a new Topic Consumer
func (t *Topic[Msg]) NewConsumer() topic.Consumer[Msg] {
	return makeConsumer(t.makeCursor())
}

// get consumes a message starting at the specified virtual Offset within the
// Topic. If the offset is no longer being retained, the next available offset
// will be consumed. The actual offset read is returned
func (t *Topic[Msg]) get(o uint64) (Msg, uint64, bool) {
	defer t.vacuumReady.Notify()
	e, o, ok := t.log.get(o)
	return e.msg, o, ok
}

// put adds the specified Message to the Topic
func (t *Topic[Msg]) put(msg Msg) {
	t.log.put(msg)
	t.notifyObservers()
}

func (t *Topic[_]) startVacuuming() {
	vacuumID := uuid.New()
	ready := channel.MakeReadyWait()
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

func (t *Topic[Msg]) vacuum() {
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

func (t *Topic[Msg]) makeCursor() *cursor[Msg] {
	c := makeCursor(t)
	t.cursors.track(c)
	t.observers.add(c.id, c.ready.Notify)
	return c
}

func (t *Topic[_]) notifyObservers() {
	t.observers.notify()
}

func makeLogObservers() *topicObservers {
	return &topicObservers{
		callbacks: map[uuid.UUID]func(){},
	}
}

func (o *topicObservers) add(i uuid.UUID, cb func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.callbacks[i] = cb
}

func (o *topicObservers) remove(i uuid.UUID) {
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
