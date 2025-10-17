package debug

import (
	"github.com/kode4food/caravan/debug"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

// Error messages
const (
	ErrProcessorReturnedEarly = "processor returned before context closed"
)

// ProcessorReturnedEarly will report a Fatal error to the Context.Monitor
// channel if the wrapped stream.Processor returns before the Context.Done
// channel is closed. This is useful to debug processors that are meant to
// perform a continuous loop over their Context.In channel. Note that not all
// processors need to perform a continuous loop. For example, node.Bind simply
// plumbs a channel between two paired processors and returns
func ProcessorReturnedEarly[In, Out any](
	p stream.Processor[In, Out],
) stream.Processor[In, Out] {
	if !debug.IsEnabled() {
		return p
	}

	return func(c *context.Context[In, Out]) {
		p(c)
		if !c.IsDone() {
			c.Fatalf(ErrProcessorReturnedEarly)
		}
	}
}
