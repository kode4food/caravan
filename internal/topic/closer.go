package topic

import (
	"sync"

	"github.com/kode4food/caravan/closer"
)

// Closer is an internal implementation of a closer.Closer
type Closer struct {
	sync.Mutex
	channel chan struct{}
	close   func()
}

func makeCloser(close func()) closer.Closer {
	return &Closer{
		channel: make(chan struct{}),
		close:   close,
	}
}

// Close will instruct the Closer to stop and free its resources
func (c *Closer) Close() {
	c.Lock()
	defer c.Unlock()
	select {
	case <-c.channel:
		return
	default:
		close(c.channel)
		c.close()
	}
}

// IsClosed returns a channel that can participate in a select
func (c *Closer) IsClosed() <-chan struct{} {
	return c.channel
}
