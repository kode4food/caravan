package context

import (
	"fmt"
	"runtime/debug"
)

type (
	// Advice reports processor state to a Stream monitor
	Advice interface {
		advice()
	}

	// Context carries lifecycle and message channels through a Processor
	Context[In, Out any] struct {
		Done    <-chan Done
		Monitor chan<- Advice
		In      <-chan In
		Out     chan<- Out
	}

	// Done signals that Stream processing has stopped
	Done struct{}

	// Supported Monitor Advice

	// Stop instructs the Stream to completely stop operating
	Stop struct{}

	// Debug reports diagnostic context from a Processor
	Debug struct {
		Message string
		Stack   []byte
	}

	// Error reports a recoverable error to the Stream
	Error struct{ error }

	// Fatal reports an error that stops the Stream
	Fatal struct{ error }
)

// New returns a processor Context over the provided channels
func New[In, Out any](
	done <-chan Done, monitor chan<- Advice, in <-chan In, out chan<- Out,
) *Context[In, Out] {
	return &Context[In, Out]{
		Done:    done,
		Monitor: monitor,
		In:      in,
		Out:     out,
	}
}

// With returns a Context with replacement input and output channels
func With[OldIn, OldOut, In, Out any](
	c *Context[OldIn, OldOut], in chan In, out chan Out,
) *Context[In, Out] {
	return New(c.Done, c.Monitor, in, out)
}

// WithIn returns a Context with a replacement input channel
func WithIn[OldIn, Out, In any](
	c *Context[OldIn, Out], in chan In,
) *Context[In, Out] {
	return New(c.Done, c.Monitor, in, c.Out)
}

// WithOut returns a Context with a replacement output channel
func WithOut[In, OldOut, Out any](
	c *Context[In, OldOut], out chan Out,
) *Context[In, Out] {
	return New(c.Done, c.Monitor, c.In, out)
}

// IsDone returns whether Stream processing has stopped
func (c *Context[In, Out]) IsDone() bool {
	select {
	case <-c.Done:
		return true
	default:
		return false
	}
}

// FetchMessage waits for the next input or for processing to stop
func (c *Context[In, Out]) FetchMessage() (In, bool) {
	select {
	case <-c.Done:
		var zero In
		return zero, false
	case msg := <-c.In:
		return msg, true
	}
}

// ForwardResult sends a result unless processing has stopped
func (c *Context[In, Out]) ForwardResult(res Out) bool {
	select {
	case <-c.Done:
		return false
	case c.Out <- res:
		return true
	}
}

// Advise reports Advice unless processing has stopped
func (c *Context[In, Out]) Advise(a Advice) bool {
	select {
	case <-c.Done:
		return false
	case c.Monitor <- a:
		return true
	}
}

// Debugf reports formatted diagnostic context
func (c *Context[_, _]) Debugf(format string, v ...any) bool {
	return c.Advise(&Debug{
		Message: fmt.Sprintf(format, v...),
		Stack:   debug.Stack(),
	})
}

// Errorf reports a formatted recoverable error
func (c *Context[_, _]) Errorf(format string, v ...any) bool {
	return c.Error(fmt.Errorf(format, v...))
}

// Error reports a recoverable error
func (c *Context[_, _]) Error(err error) bool {
	return c.Advise(&Error{err})
}

// Fatalf reports a formatted error that stops the Stream
func (c *Context[_, _]) Fatalf(format string, v ...any) bool {
	return c.Fatal(fmt.Errorf(format, v...))
}

// Fatal reports an error that stops the Stream
func (c *Context[_, _]) Fatal(err error) bool {
	return c.Advise(&Fatal{err})
}

func (Stop) advice()   {}
func (*Debug) advice() {}
func (*Error) advice() {}
func (*Fatal) advice() {}
