package debug

import internal "github.com/kode4food/caravan/internal/topic"

var (
	// Enable enables debugging information to be emitted
	Enable = internal.Debug.Enable

	// IsEnabled returns whether debugging information is enabled
	IsEnabled = internal.Debug.IsEnabled

	// WithConsumer provides a debugging Consumer to the provided callback
	// function. That Consumer is managed by the call to WithConsumer, so
	// it need not be closed explicitly by the caller
	WithConsumer = internal.Debug.WithConsumer

	// TailLogTo begins to stream any new debugging information to the
	// specified io.Writer. Debugging information must be enabled for this
	// call to produce any output
	TailLogTo = internal.Debug.TailLogTo
)
