package mutex_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/internal/sync/mutex"
)

func TestInitialMutexBasics(t *testing.T) {
	as := assert.New(t)
	done := make(chan bool)

	var m mutex.InitialMutex
	m.Lock()
	res := false
	go func() {
		m.Lock()
		as.False(m.IsLockDisabled())
		as.False(res)
		res = true
		m.DisableLock()
	}()
	as.False(res)
	m.Unlock()
	time.Sleep(10 * time.Millisecond)
	go func() {
		m.Lock()
		as.True(m.IsLockDisabled())
		as.True(res)
		m.Unlock()
	}()

	go func() {
		time.Sleep(50 * time.Millisecond)
		done <- true
	}()
	<-done
}

func TestInitialMutexNoDeadlock(t *testing.T) {
	as := assert.New(t)

	var m mutex.InitialMutex
	as.False(m.IsLockDisabled())
	m.DisableLock()
	as.True(m.IsLockDisabled())

	// Nothing beyond here should block or panic
	m.Lock()
	m.Lock()
	m.Unlock() //lint:ignore SA2001 intentional
	m.Unlock()
	m.DisableLock()
	m.Lock()
	m.Unlock() //lint:ignore SA2001 intentional
}

func TestInitialMutexDisableRace(t *testing.T) {
	as := assert.New(t)

	var m mutex.InitialMutex
	m.Lock()
	done := make(chan bool)
	go func() {
		m.Lock()
		as.True(m.IsLockDisabled())
		m.Lock() // now it should be disabled
		m.Lock()
		m.Unlock() //lint:ignore SA2001 intentional
		done <- true
	}()
	time.Sleep(500 * time.Millisecond)
	m.DisableLock()
	<-done
}
