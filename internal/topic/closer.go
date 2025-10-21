package topic

import "github.com/kode4food/caravan/closer"

type Closer struct {
	closed  chan struct{}
	onClose func()
}

func makeCloser(onClose func()) closer.Closer {
	return &Closer{
		closed:  make(chan struct{}),
		onClose: onClose,
	}
}

func (c *Closer) Close() {
	select {
	case <-c.closed:
		return
	default:
		close(c.closed)
		if c.onClose != nil {
			c.onClose()
		}
	}
}

func (c *Closer) IsClosed() <-chan struct{} {
	return c.closed
}
