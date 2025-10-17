package context

import (
	"fmt"
	"runtime/debug"
)

type (
	Advice interface {
		advice()
	}

	Context[In, Out any] struct {
		Done    <-chan Done
		Monitor chan<- Advice
		In      <-chan In
		Out     chan<- Out
	}

	Done struct{}

	// Supported Monitor Advice

	// Stop is Advice that instructs the Stream to completely stop operating.
	// This should only be used in exceptional cases.
	Stop struct{}

	Debug struct {
		Message string
		Stack   []byte
	}

	// Error is Advice that reports a recoverable error to the Stream.
	Error struct{ error }

	// Fatal is Advice that reports a non-recoverable error to the Stream. The
	// Stream will be stopped when encountering such an error.
	Fatal struct{ error }
)

func Make[In, Out any](
	done <-chan Done, monitor chan<- Advice, in <-chan In, out chan<- Out,
) *Context[In, Out] {
	return &Context[In, Out]{done, monitor, in, out}
}

func With[OldIn, OldOut, In, Out any](
	c *Context[OldIn, OldOut], in chan In, out chan Out,
) *Context[In, Out] {
	return Make(c.Done, c.Monitor, in, out)
}

func WithIn[OldIn, Out, In any](
	c *Context[OldIn, Out], in chan In,
) *Context[In, Out] {
	return Make(c.Done, c.Monitor, in, c.Out)
}

func WithOut[In, OldOut, Out any](
	c *Context[In, OldOut], out chan Out,
) *Context[In, Out] {
	return Make(c.Done, c.Monitor, c.In, out)
}

func (c *Context[In, Out]) IsDone() bool {
	select {
	case <-c.Done:
		return true
	default:
		return false
	}
}

func (c *Context[In, Out]) FetchMessage() (In, bool) {
	select {
	case <-c.Done:
		var zero In
		return zero, false
	case msg := <-c.In:
		return msg, true
	}
}

func (c *Context[In, Out]) ForwardResult(res Out) bool {
	select {
	case <-c.Done:
		return false
	case c.Out <- res:
		return true
	}
}

func (c *Context[In, Out]) Advise(a Advice) bool {
	select {
	case <-c.Done:
		return false
	case c.Monitor <- a:
		return true
	}
}

func (c *Context[_, _]) Debugf(format string, v ...any) bool {
	return c.Advise(&Debug{
		Message: fmt.Sprintf(format, v...),
		Stack:   debug.Stack(),
	})
}

func (c *Context[_, _]) Errorf(format string, v ...any) bool {
	return c.Error(fmt.Errorf(format, v...))
}

func (c *Context[_, _]) Error(err error) bool {
	return c.Advise(&Error{err})
}

func (c *Context[_, _]) Fatalf(format string, v ...any) bool {
	return c.Fatal(fmt.Errorf(format, v...))
}

func (c *Context[_, _]) Fatal(err error) bool {
	return c.Advise(&Fatal{err})
}

func (Stop) advice()   {}
func (*Debug) advice() {}
func (*Error) advice() {}
func (*Fatal) advice() {}
