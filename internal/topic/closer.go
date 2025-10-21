package topic

import "github.com/kode4food/caravan/closer"

type Closer struct {
	channel chan struct{}
	close   func()
}

func makeCloser(close func()) closer.Closer {
	return &Closer{
		channel: make(chan struct{}),
		close:   close,
	}
}

func (c *Closer) Close() {
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

func (c *Closer) IsClosed() <-chan struct{} {
	return c.channel
}
