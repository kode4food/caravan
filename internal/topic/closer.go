package topic

import "github.com/kode4food/caravan/closer"

type topicCloser struct {
	channel chan struct{}
	close   func()
}

func newCloser(close func()) closer.Closer {
	return &topicCloser{
		channel: make(chan struct{}),
		close:   close,
	}
}

// Close releases the Topic resource
func (c *topicCloser) Close() {
	select {
	case <-c.channel:
		return
	default:
		close(c.channel)
		if c.close != nil {
			c.close()
		}
	}
}

// IsClosed returns a channel closed with the resource
func (c *topicCloser) IsClosed() <-chan struct{} {
	return c.channel
}
