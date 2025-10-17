package channel

import "sync"

// ReadyWait is a structure that manages a channel to be used for simple
// readiness notification. The value of a structure like this over a Cond is
// that a channel can participate in a select
type ReadyWait struct {
	mutex sync.Mutex
	ready chan struct{}
}

const readyWaitCap = 1 // must be non-zero

// MakeReadyWait returns a new ReadyWait
func MakeReadyWait() *ReadyWait {
	return &ReadyWait{
		ready: make(chan struct{}, readyWaitCap),
	}
}

// Notify wakes up any process waiting on the ready channel without blocking
// the calling routine
func (r *ReadyWait) Notify() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if len(r.ready) < cap(r.ready) {
		r.ready <- struct{}{}
	}
}

// Wait returns the underlying channel and can be used to wait for the Notify
// method having been called
func (r *ReadyWait) Wait() <-chan struct{} {
	return r.ready
}

// Close closes the underlying ready channel
func (r *ReadyWait) Close() {
	close(r.ready)
}
