package topic

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/kode4food/caravan/closer"
	"github.com/kode4food/caravan/internal/sync/channel"
)

type (
	// cursors manages a set of cursors on behalf of a Topic
	cursors[Msg any] struct {
		sync.RWMutex
		cursors map[uuid.UUID]*cursor[Msg]
	}

	// cursor is used to consume log entries
	cursor[Msg any] struct {
		closer.Closer
		id     uuid.UUID
		topic  *Topic[Msg]
		ready  *channel.ReadyWait
		offset uint64
	}
)

func makeCursor[Msg any](t *Topic[Msg]) *cursor[Msg] {
	cID := uuid.New()
	ready := channel.MakeReadyWait()
	if t.Length() != 0 {
		ready.Notify()
	}

	return &cursor[Msg]{
		id:    cID,
		topic: t,
		ready: ready,
		Closer: makeCloser(func() {
			ready.Close()
			t.cursors.remove(cID)
			t.observers.remove(cID)
		}),
	}
}

func (c *cursor[Msg]) head() (Msg, bool) {
	off := atomic.LoadUint64(&c.offset)
	if e, o, ok := c.topic.get(off); ok {
		atomic.StoreUint64(&c.offset, o)
		return e, true
	}
	var zero Msg
	return zero, false
}

func (c *cursor[_]) advance() {
	atomic.AddUint64(&c.offset, 1)
}

func makeCursors[Msg any]() *cursors[Msg] {
	return &cursors[Msg]{
		cursors: map[uuid.UUID]*cursor[Msg]{},
	}
}

func (c *cursors[Msg]) track(cursor *cursor[Msg]) {
	c.Lock()
	defer c.Unlock()
	i := cursor.id
	if _, ok := c.cursors[i]; !ok {
		c.cursors[i] = cursor
	}
}

func (c *cursors[_]) remove(i uuid.UUID) {
	c.Lock()
	defer c.Unlock()
	delete(c.cursors, i)
}

func (c *cursors[_]) offsets() []uint64 {
	c.RLock()
	defer c.RUnlock()
	res := make([]uint64, 0, len(c.cursors))
	for _, cursor := range c.cursors {
		off := atomic.LoadUint64(&cursor.offset)
		res = append(res, off)
	}
	return res
}
