package mutex

import (
	"sync"
	"sync/atomic"
)

// InitialMutex is a mutex that can be bypassed after mutation finishes
type InitialMutex struct {
	mu    sync.Mutex
	state atomic.Int32
}

const (
	disabled int32 = iota - 1
	unlocked
	locked
)

// DisableLock instructs the InitialMutex to ignore all subsequent calls to
// Lock and Unlock
func (m *InitialMutex) DisableLock() {
	switch m.state.Load() {
	case disabled:
		return
	case locked:
		m.state.Store(disabled)
		m.mu.Unlock()
	case unlocked:
		m.mu.Lock()
		m.state.Store(disabled)
		m.mu.Unlock()
	}
}

// IsLockDisabled returns whether locking has been disabled
func (m *InitialMutex) IsLockDisabled() bool {
	return m.state.Load() == disabled
}

// Lock potentially locks this InitialMutex, if enabled
func (m *InitialMutex) Lock() {
	if m.state.Load() == disabled {
		return
	}
	m.mu.Lock()
	if m.state.Load() == disabled {
		m.mu.Unlock()
		return
	}
	m.state.Store(locked)
}

// Unlock potentially unlocks this InitialMutex, if enabled
func (m *InitialMutex) Unlock() {
	if m.state.Load() == locked {
		m.state.Store(unlocked)
		m.mu.Unlock()
	}
}

// Reset places the InitialMutex back in its initial unlocked, enabled state
func (m *InitialMutex) Reset() {
	if m.state.Load() == disabled {
		m.state.Store(unlocked)
	}
}
