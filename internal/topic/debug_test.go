package topic_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/topic"

	internal "github.com/kode4food/caravan/internal/topic"
)

func TestDebugProducerClose(t *testing.T) {
	as := assert.New(t)
	internal.Debug.Enable()

	internal.Debug.WithConsumer(func(c topic.Consumer[error]) {
		top := internal.Make[any]()
		top.NewProducer()
		runtime.GC()

		errs := c.Receive()
		as.Error(<-errs)
	})
}

func TestDebugConsumerClose(t *testing.T) {
	as := assert.New(t)
	internal.Debug.Enable()

	internal.Debug.WithConsumer(func(c topic.Consumer[error]) {
		top := internal.Make[any]()
		top.NewConsumer()
		runtime.GC()

		errs := c.Receive()
		as.Error(<-errs)
	})
}
