package topic

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/kode4food/caravan/internal/sync/channel"
	"github.com/kode4food/caravan/topic"
	"github.com/kode4food/caravan/topic/backoff"
	"github.com/kode4food/caravan/topic/config"
	"github.com/kode4food/caravan/topic/retention"
)

type (
	// Topic is the internal implementation of a Topic
	Topic[Msg any] struct {
		*config.Config
		retentionState retention.State
		log            *Log[Msg]
		cursors        *cursors[Msg]
		observers      *topicObservers
		vacuumReady    *channel.ReadyWait
	}

	// topicObservers manages a set of callbacks for observers of a Topic
	topicObservers struct {
		sync.RWMutex
		callbacks map[uuid.UUID]func()
	}
)

// Make instantiates a new internal Topic instance
func Make[Msg any](o ...config.Option) topic.Topic[Msg] {
	cfg := &config.Config{}
	withDefaults := append(o, config.Defaults)
	if err := config.ApplyOptions(cfg, withDefaults...); err != nil {
		panic(err)
	}

	res := &Topic[Msg]{
		Config:         cfg,
		retentionState: cfg.RetentionPolicy.InitialState(),
		cursors:        makeCursors[Msg](),
		observers:      makeLogObservers(),
		log:            makeLog[Msg](cfg),
	}

	res.startVacuuming()
	return res
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
	return makeConsumer(t.makeCursor(), t.BackoffGenerator)
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

func (t *Topic[_]) isClosed() bool {
	return false
}

func (t *Topic[_]) startVacuuming() {
	vacuumID := uuid.New()
	ready := channel.MakeReadyWait()
	t.vacuumReady = ready
	t.observers.add(vacuumID, ready.Notify)

	go func() {
		b := backoff.DefaultGenerator
		next := b()
		for !t.isClosed() {
			select {
			case <-time.After(next()):
			case <-ready.Wait():
			}
			if t.log.canVacuum() {
				t.vacuum()
				next = b()
			}
		}
	}()
}

func (t *Topic[Msg]) vacuum() {
	baseStats := t.baseRetentionStatistics()
	t.log.vacuum(func(e *segment[Msg]) bool {
		start := t.log.start()
		firstTimestamp, lastTimestamp := e.timeRange()
		stats := *baseStats()
		stats.Entries = &retention.EntriesStatistics{
			FirstOffset:    start,
			LastOffset:     start + uint64(e.length()-1),
			FirstTimestamp: firstTimestamp,
			LastTimestamp:  lastTimestamp,
		}
		s, r := t.RetentionPolicy.Retain(t.retentionState, &stats)
		t.retentionState = s
		return r
	})
}

func (t *Topic[_]) baseRetentionStatistics() func() *retention.Statistics {
	var base *retention.Statistics
	return func() *retention.Statistics {
		if base == nil {
			base = &retention.Statistics{
				CurrentTime: time.Now(),
				Log: &retention.LogStatistics{
					Length:        t.log.length(),
					CursorOffsets: t.cursors.offsets(),
				},
			}
		}
		return base
	}
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
	o.Lock()
	defer o.Unlock()
	o.callbacks[i] = cb
}

func (o *topicObservers) remove(i uuid.UUID) {
	o.Lock()
	defer o.Unlock()
	delete(o.callbacks, i)
}

func (o *topicObservers) notify() {
	o.RLock()
	defer o.RUnlock()
	for _, cb := range o.callbacks {
		cb()
	}
}
