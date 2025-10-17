package closer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/closer"
)

// mockCloser is a test implementation of the Closer interface
type mockCloser struct {
	closed chan struct{}
}

func newMockCloser() *mockCloser {
	return &mockCloser{
		closed: make(chan struct{}),
	}
}

func (c *mockCloser) Close() {
	select {
	case <-c.closed:
		// Already closed
	default:
		close(c.closed)
	}
}

func (c *mockCloser) IsClosed() <-chan struct{} {
	return c.closed
}

func TestIsClosed(t *testing.T) {
	as := assert.New(t)

	// Test with an open closer
	c := newMockCloser()
	as.False(closer.IsClosed(c), "Should return false for open closer")

	// Close the closer
	c.Close()
	as.True(closer.IsClosed(c), "Should return true for closed closer")
}

func TestIsClosedMultipleCalls(t *testing.T) {
	as := assert.New(t)

	c := newMockCloser()

	// Check multiple times while open
	as.False(closer.IsClosed(c))
	as.False(closer.IsClosed(c))
	as.False(closer.IsClosed(c))

	// Close and check multiple times
	c.Close()
	as.True(closer.IsClosed(c))
	as.True(closer.IsClosed(c))
	as.True(closer.IsClosed(c))
}

func TestIsClosedWithChannelSelect(t *testing.T) {
	as := assert.New(t)

	c := newMockCloser()

	// Test that IsClosed can be used in a select statement
	select {
	case <-c.IsClosed():
		as.Fail("Should not be closed yet")
	default:
		// Expected path
	}

	// Close and test again
	c.Close()

	select {
	case <-c.IsClosed():
		// Expected path
	default:
		as.Fail("Should be closed now")
	}
}
