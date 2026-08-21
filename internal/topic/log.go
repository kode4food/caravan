package topic

import (
	"sync"
	"sync/atomic"

	"github.com/kode4food/caravan/internal/sync/mutex"
)

type (
	log[Msg any] struct {
		tail          tailSegment[Msg]
		head          headSegment[Msg]
		startOffset   uint64
		virtualLength atomic.Uint64
		capIncrement  uint32
		segmentPool   sync.Pool
	}

	headSegment[Msg any] struct {
		segment *segment[Msg]
		mu      sync.RWMutex
	}

	tailSegment[Msg any] struct {
		segment *segment[Msg]
		mu      sync.Mutex
	}

	// segment manages a set of Log entries that include messages and the
	// Time at which they were emitted
	segment[Msg any] struct {
		log     *log[Msg]
		next    *segment[Msg]
		entries []Msg
		mu      mutex.InitialMutex
		len     uint32
		cap     uint32
	}

	// retentionQuery is called by Log in order to determine if a segment
	// should be retained or discarded based on cursor positions
	retentionQuery[Msg any] func(*segment[Msg]) bool
)

func newLog[Msg any](segmentSize uint32) *log[Msg] {
	l := &log[Msg]{
		capIncrement: segmentSize,
	}
	l.segmentPool.New = func() any {
		return l.newSegment()
	}
	return l
}

func (l *log[_]) start() uint64 {
	return atomic.LoadUint64(&l.startOffset)
}

func (l *log[_]) length() uint64 {
	return l.virtualLength.Load()
}

func (l *log[Msg]) put(msg Msg) {
	l.tail.mu.Lock()
	defer l.tail.mu.Unlock()
	tail := l.tail.segment
	if tail == nil {
		l.head.mu.Lock()
		defer l.head.mu.Unlock()
		tail = l.getSegment()
		l.head.segment = tail
		l.tail.segment = tail
	}
	if s := tail.append(msg); s != tail {
		l.tail.segment = s
	}
	l.virtualLength.Add(uint64(1))
}

func (l *log[Msg]) getSegment() *segment[Msg] {
	s := l.segmentPool.Get().(*segment[Msg])
	s.next = nil
	s.len = 0
	s.mu.Reset()
	return s
}

func (l *log[Msg]) newSegment() *segment[Msg] {
	c := l.capIncrement
	return &segment[Msg]{
		log:     l,
		cap:     c,
		entries: make([]Msg, c),
	}
}

func (l *log[Msg]) returnSegment(s *segment[Msg]) {
	l.segmentPool.Put(s)
}

func (l *log[Msg]) get(o uint64) (Msg, uint64, bool) {
	l.head.mu.RLock()
	o, pos := l.relativePos(o)
	curr := l.head.segment
	l.head.mu.RUnlock()

	for ; curr != nil && pos >= uint64(curr.cap); curr = curr.getNext() {
		pos -= uint64(curr.cap)
	}
	if curr != nil {
		p := int(pos)
		if p < int(curr.length()) {
			return curr.entries[p], o, true
		}
	}
	var zero Msg
	return zero, o, false
}

func (l *log[_]) relativePos(o uint64) (uint64, uint64) {
	eo := l.startOffset
	if o < eo { // if requested is less than actual, we start at actual
		o = eo
	}
	return o, o - eo
}

func (l *log[_]) canVacuum() bool {
	l.head.mu.RLock()
	defer l.head.mu.RUnlock()
	if head := l.head.segment; head != nil {
		return !head.isActive()
	}
	return false
}

func (l *log[Msg]) vacuum(retain retentionQuery[Msg]) {
	l.head.mu.Lock()
	defer l.head.mu.Unlock()

	for curr := l.head.segment; curr != nil; {
		if curr.isActive() || retain(curr) {
			return
		}
		ret := curr
		l.startOffset += uint64(curr.cap)
		if curr = curr.getNext(); curr != nil {
			l.head.segment = curr
			l.returnSegment(ret)
			continue
		}
		l.tail.mu.Lock()
		l.head.segment = nil
		l.tail.segment = nil
		l.tail.mu.Unlock()
		l.returnSegment(ret)
		return
	}
}

func (s *segment[Msg]) getNext() *segment[Msg] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

func (s *segment[Msg]) append(msg Msg) *segment[Msg] {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.len == s.cap {
		s.next = s.log.getSegment()
		s.mu.DisableLock()
		return s.next.append(msg)
	}
	s.entries[s.len] = msg
	atomic.AddUint32(&s.len, uint32(1))
	return s
}

func (s *segment[_]) length() uint32 {
	return atomic.LoadUint32(&s.len)
}

func (s *segment[_]) isActive() bool {
	return !s.isFull()
}

func (s *segment[_]) isFull() bool {
	return s.length() == s.cap
}
