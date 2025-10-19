package topic_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/debug"
	"github.com/kode4food/caravan/topic"
)

func TestDebugProducerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c topic.Consumer[error]) {
		top := caravan.NewTopic[any]()
		top.NewProducer()
		runtime.GC()

		errs := c.Receive()
		as.Error(<-errs)
	})
}

func TestDebugConsumerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c topic.Consumer[error]) {
		top := caravan.NewTopic[any]()
		top.NewConsumer()
		runtime.GC()

		errs := c.Receive()
		as.Error(<-errs)
	})
}
