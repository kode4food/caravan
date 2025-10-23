package topic

import (
	"sync"
	"sync/atomic"

	"github.com/kode4food/caravan/internal/sync/mutex"
)

type (
	// Log manages a set of segments that contain Log entries
	Log[Msg any] struct {
		tail          tailSegment[Msg]
		head          headSegment[Msg]
		startOffset   uint64
		virtualLength uint64
		capIncrement  uint32
	}

	logEntry[Msg any] struct {
		msg Msg
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
		log     *Log[Msg]
		next    *segment[Msg]
		entries []*logEntry[Msg]
		mu      mutex.InitialMutex
		len     uint32
		cap     uint32
	}

	// retentionQuery is called by Log in order to determine if a segment
	// should be retained or discarded based on cursor positions
	retentionQuery[Msg any] func(*segment[Msg]) bool
)

func makeLog[Msg any](segmentSize uint32) *Log[Msg] {
	return &Log[Msg]{
		capIncrement: segmentSize,
	}
}

func (l *Log[_]) start() uint64 {
	return atomic.LoadUint64(&l.startOffset)
}

func (l *Log[_]) length() uint64 {
	return atomic.LoadUint64(&l.virtualLength)
}

func (l *Log[_]) nextCapacity() uint32 {
	return l.capIncrement
}

func (l *Log[Msg]) put(msg Msg) {
	entry := &logEntry[Msg]{
		msg: msg,
	}

	l.tail.mu.Lock()
	defer l.tail.mu.Unlock()
	tail := l.tail.segment
	if tail == nil {
		l.head.mu.Lock()
		defer l.head.mu.Unlock()
		tail = l.makeSegment()
		l.head.segment = tail
		l.tail.segment = tail
	}
	if s := tail.append(entry); s != tail {
		l.tail.segment = s
	}
	atomic.AddUint64(&l.virtualLength, uint64(1))
}

func (l *Log[Msg]) makeSegment() *segment[Msg] {
	c := l.nextCapacity()
	return &segment[Msg]{
		log:     l,
		cap:     c,
		entries: make([]*logEntry[Msg], c),
	}
}

func (l *Log[Msg]) get(o uint64) (*logEntry[Msg], uint64, bool) {
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
	return &logEntry[Msg]{}, o, false
}

func (l *Log[_]) relativePos(o uint64) (uint64, uint64) {
	eo := l.startOffset
	if o < eo { // if requested is less than actual, we start at actual
		o = eo
	}
	return o, o - eo
}

func (l *Log[_]) canVacuum() bool {
	l.head.mu.RLock()
	defer l.head.mu.RUnlock()
	if head := l.head.segment; head != nil {
		return !head.isActive()
	}
	return false
}

func (l *Log[Msg]) vacuum(retain retentionQuery[Msg]) {
	l.head.mu.Lock()
	defer l.head.mu.Unlock()

	for curr := l.head.segment; curr != nil; {
		if curr.isActive() || retain(curr) {
			return
		}
		l.startOffset += uint64(curr.cap)
		if curr = curr.getNext(); curr != nil {
			l.head.segment = curr
			continue
		}
		l.tail.mu.Lock()
		l.head.segment = nil
		l.tail.segment = nil
		l.tail.mu.Unlock()
		return
	}
}

func (s *segment[Msg]) getNext() *segment[Msg] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

func (s *segment[Msg]) append(entry *logEntry[Msg]) *segment[Msg] {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.len == s.cap {
		s.next = s.log.makeSegment()
		s.mu.DisableLock()
		return s.next.append(entry)
	}
	s.entries[s.len] = entry
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
