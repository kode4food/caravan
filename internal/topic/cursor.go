package topic

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/kode4food/caravan/closer"
	"github.com/kode4food/caravan/internal/sync/channel"
)

type (
	// cursors manages a set of cursors on behalf of a Topic
	cursors[Msg any] struct {
		cursors map[uint64]*cursor[Msg]
		mu      sync.RWMutex
	}

	// cursor is used to consume log entries
	cursor[Msg any] struct {
		closer.Closer
		topic  *topicState[Msg]
		ready  *channel.ReadyWait
		offset atomic.Uint64
		id     uint64
	}
)

func newCursor[Msg any](t *topicState[Msg]) *cursor[Msg] {
	cID := t.nextID.Add(1)
	ready := channel.NewReadyWait()
	if t.Length() != 0 {
		ready.Notify()
	}

	res := &cursor[Msg]{
		id:    cID,
		topic: t,
		ready: ready,
		Closer: newCloser(func() {
			ready.Close()
			t.cursors.remove(cID)
			t.observers.remove(cID)
		}),
	}
	runtime.SetFinalizer(res, cursorFinalizer[Msg])
	return res
}

func (c *cursor[Msg]) head() (Msg, bool) {
	off := c.offset.Load()
	if e, o, ok := c.topic.get(off); ok {
		c.offset.Store(o)
		return e, true
	}
	var zero Msg
	return zero, false
}

func (c *cursors[Msg]) track(cursor *cursor[Msg]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := cursor.id
	if _, ok := c.cursors[i]; !ok {
		c.cursors[i] = cursor
	}
}

func (c *cursors[_]) remove(i uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cursors, i)
}

func (c *cursors[_]) offsets() []uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]uint64, 0, len(c.cursors))
	for _, cursor := range c.cursors {
		off := cursor.offset.Load()
		res = append(res, off)
	}
	return res
}

func cursorFinalizer[Msg any](c *cursor[Msg]) {
	select {
	case <-c.IsClosed():
	default:
		c.Close()
	}
}
