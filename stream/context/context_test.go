package context_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/stream/context"
)

func TestOpenContext(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	monitor := make(chan context.Advice)
	in := make(chan any)
	out := make(chan any)

	c := context.Make(done, monitor, in, out)
	as.NotNil(c)
	as.False(c.IsDone())

	go func() { in <- "hello" }()
	msg, ok := c.FetchMessage()
	as.True(ok)
	as.Equal("hello", msg)

	go func() {
		ok = c.ForwardResult("goodbye")
		as.True(ok)
	}()
	as.Equal("goodbye", <-out)

	go func() {
		ok = c.Errorf("hello")
		as.True(ok)
	}()
	as.EqualError((<-monitor).(error), "hello")
}

func TestClosedContext(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	close(done)

	in := make(chan any)
	out := make(chan any)

	c := context.Make(done, make(chan context.Advice), in, out)
	as.NotNil(c)
	as.True(c.IsDone())

	go func() { in <- "hello" }()
	msg, ok := c.FetchMessage()
	as.False(ok)
	as.Equal(nil, msg)

	ok = c.ForwardResult("goodbye")
	as.False(ok)

	ok = c.Errorf("hello")
	as.False(ok)
}

func TestContextWith(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)

	c1 := context.Make[any, any](done, make(chan context.Advice), nil, nil)
	as.Nil(c1.In)
	as.Nil(c1.Out)

	c2 := context.WithIn(c1, make(chan any))
	as.Nil(c2.Out)
	as.NotNil(c2.In)
	as.Nil(c1.In)

	c3 := context.WithOut(c2, make(chan any))
	as.NotNil(c3.Out)
	as.NotNil(c3.In)
	as.Nil(c2.Out)
	as.Equal(c3.In, c2.In)

	c4 := context.With(c3, make(chan any), make(chan any))
	as.NotNil(c4.Out)
	as.NotNil(c4.Out)
	as.NotEqual(c3.Out, c4.Out)
	as.NotEqual(c3.In, c4.In)
}

func TestMonitorMessages(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	monitor := make(chan context.Advice)
	c1 := context.Make[any, any](done, monitor, nil, nil)

	go func() {
		c1.Advise(context.Stop{})
		c1.Errorf("recoverable error")
		c1.Fatalf("fatal error")
	}()

	go func() {
		v1, ok := (<-monitor).(context.Stop)
		as.NotNil(v1)
		as.True(ok)

		v2, ok := (<-monitor).(*context.Error)
		as.NotNil(v2)
		as.True(ok)
		as.EqualError(v2, "recoverable error")

		v3, ok := (<-monitor).(*context.Fatal)
		as.NotNil(v3)
		as.True(ok)
		as.EqualError(v3, "fatal error")

		close(done)
	}()

	<-done
}
