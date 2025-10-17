package topic

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kode4food/caravan/internal/sync/mutex"
	"github.com/kode4food/caravan/topic/config"
)

type (
	// Log manages a set of segments that contain Log entries
	Log[Msg any] struct {
		startOffset   uint64
		virtualLength uint64
		capIncrement  uint32
		head          headSegment[Msg]
		tail          tailSegment[Msg]
	}

	logEntry[Msg any] struct {
		msg       Msg
		createdAt time.Time
	}

	headSegment[Msg any] struct {
		sync.RWMutex
		segment *segment[Msg]
	}

	tailSegment[Msg any] struct {
		sync.Mutex
		segment *segment[Msg]
	}

	// segment manages a set of Log entries that include messages and the
	// Time at which they were emitted
	segment[Msg any] struct {
		mutex.InitialMutex
		log     *Log[Msg]
		next    *segment[Msg]
		len     uint32
		cap     uint32
		entries []*logEntry[Msg]
	}

	// retentionQuery is called by Log in order to determine if a segment
	// should be retained or discarded. Such a function is provided by a
	// Topic in order to apply a retention.Policy
	retentionQuery[Msg any] func(*segment[Msg]) bool
)

func makeLog[Msg any](cfg *config.Config) *Log[Msg] {
	return &Log[Msg]{
		capIncrement: uint32(cfg.SegmentIncrement),
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
		msg:       msg,
		createdAt: time.Now(),
	}

	l.tail.Lock()
	defer l.tail.Unlock()
	tail := l.tail.segment
	if tail == nil {
		l.head.Lock()
		defer l.head.Unlock()
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
	l.head.RLock()
	o, pos := l.relativePos(o)
	curr := l.head.segment
	l.head.RUnlock()

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
	l.head.RLock()
	defer l.head.RUnlock()
	if head := l.head.segment; head != nil {
		return !head.isActive()
	}
	return false
}

func (l *Log[Msg]) vacuum(retain retentionQuery[Msg]) {
	l.head.Lock()
	defer l.head.Unlock()

	for curr := l.head.segment; curr != nil; {
		if curr.isActive() || retain(curr) {
			return // stop as soon as we see an active or retained segment
		}
		l.startOffset += uint64(curr.cap)
		if curr = curr.getNext(); curr != nil {
			l.head.segment = curr
			continue
		}
		l.tail.Lock()
		l.head.segment = nil
		l.tail.segment = nil
		l.tail.Unlock()
		return
	}
}

func (s *segment[Msg]) getNext() *segment[Msg] {
	s.Lock()
	defer s.Unlock()
	return s.next
}

func (s *segment[Msg]) append(entry *logEntry[Msg]) *segment[Msg] {
	s.Lock()
	defer s.Unlock()
	if s.len == s.cap {
		s.next = s.log.makeSegment()
		s.DisableLock()
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

func (s *segment[_]) timeRange() (time.Time, time.Time) {
	f := s.entries[0].createdAt
	l := s.entries[s.length()-1].createdAt
	return f, l
}
