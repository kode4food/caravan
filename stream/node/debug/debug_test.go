package debug_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	_debug "github.com/kode4food/caravan/debug"

	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/stream/node/debug"
)

func TestProcessorReturnedEarly(t *testing.T) {
	as := assert.New(t)

	m := debug.ProcessorReturnedEarly(func(c *context.Context[any, any]) {
		// no need to do anything
	})

	monitor := make(chan context.Advice)
	c := context.Make(
		make(chan context.Done), monitor, make(chan any), make(chan any),
	)
	m.Start(c)

	if _debug.IsEnabled() {
		as.EqualError(
			(<-monitor).(*context.Fatal),
			debug.ErrProcessorReturnedEarly,
		)
	} else {
		select {
		case <-monitor:
			as.Fail("should not have monitor advice")
		default:
			// all good
		}
	}
}
