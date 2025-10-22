package stream

import (
	"log/slog"
	"time"

	"github.com/kode4food/caravan/stream/context"
)

type (
	// Stream is a process that performs the work assigned to it using the set
	// of Processors provided to it when constructed
	Stream interface {
		// Start begins background processing of the Stream
		Start() Running

		// StartWith begins background processing of the Stream, but gives the
		// programmer first crack at the Advice being received on the Stream's
		// monitor channel
		StartWith(AdviceHandler) Running
	}

	Running interface {
		// Stop instructs the Stream to stop processing
		Stop() error

		// IsRunning returns whether the Stream is processing messages
		IsRunning() bool
	}

	// AdviceHandler is provided to Stream.StartWith so that the programmer may
	// react to Advice messages being received by the Stream's monitor channel.
	// Calling next() will ultimately invoke the default behavior for the
	// received Advice. Not calling next() will short-circuit that behavior
	AdviceHandler func(a context.Advice, next func())

	// Processor is a function that processes part of a Stream topology.
	// Recoverable and fatal errors can be sent to the context.Context's
	// Monitor channel.
	Processor[In, Out any] func(*context.Context[In, Out])

	// Source messages are provided to a Processor that is meant to generate
	// messages from a source outside its current Stream. Examples would be
	// node.TopicConsumer and node.Generate
	Source struct{}

	// Sink messages are produced by a Processor that is meant to terminate a
	// Stream. Examples would be node.SinkTo and node.Sink
	Sink struct{}
)

// Error messages
const (
	ErrReturnedLate   = "processor returned late before context closed"
	ErrAlreadyStopped = "stream already stopped"
)

// Start begins the Processor in a new go routine, logging any abnormalities
func (p Processor[In, Out]) Start(c *context.Context[In, Out]) {
	go func() {
		start := time.Now().UnixNano() / int64(time.Millisecond)
		p(c)
		end := time.Now().UnixNano() / int64(time.Millisecond)
		if end-start > 1 && !c.IsDone() {
			slog.Debug(ErrReturnedLate)
		}
	}()
}
