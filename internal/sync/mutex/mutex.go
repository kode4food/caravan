package mutex

import (
	"sync"
	"sync/atomic"
)

// InitialMutex is a mutex that can eventually be bypassed. Good for structures
// that are initially mutable, but thereafter read-only.
type InitialMutex struct {
	mu    sync.Mutex
	state int32
}

// Status constants
const (
	Disabled int32 = iota - 1
	Unlocked
	Locked
)

// DisableLock instructs the InitialMutex to ignore all subsequent calls to
// Lock and Unlock
func (m *InitialMutex) DisableLock() {
	switch atomic.LoadInt32(&m.state) {
	case Disabled:
		return
	case Locked:
		atomic.StoreInt32(&m.state, Disabled)
		m.mu.Unlock()
	case Unlocked:
		m.mu.Lock()
		atomic.StoreInt32(&m.state, Disabled)
		m.mu.Unlock()
	}
}

// IsLockDisabled returns whether locking has been disabled
func (m *InitialMutex) IsLockDisabled() bool {
	return atomic.LoadInt32(&m.state) == Disabled
}

// Lock potentially locks this InitialMutex, if enabled
func (m *InitialMutex) Lock() {
	if atomic.LoadInt32(&m.state) == Disabled {
		return
	}
	m.mu.Lock()
	if atomic.LoadInt32(&m.state) == Disabled {
		m.mu.Unlock()
		return
	}
	atomic.StoreInt32(&m.state, Locked)
}

// Unlock potentially unlocks this InitialMutex, if enabled
func (m *InitialMutex) Unlock() {
	if atomic.LoadInt32(&m.state) == Locked {
		atomic.StoreInt32(&m.state, Unlocked)
		m.mu.Unlock()
	}
}

// Reset places the InitialMutex back in its initial unlocked, enabled state
func (m *InitialMutex) Reset() {
	if atomic.LoadInt32(&m.state) == Disabled {
		atomic.StoreInt32(&m.state, Unlocked)
	}
}
