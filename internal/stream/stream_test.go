package stream_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	_stream "github.com/kode4food/caravan/internal/stream"
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

func newGeneratingStream(value any) stream.Stream {
	return _stream.New(node.Generate(func() (any, bool) { return value, true }))
}

func TestStreamCreate(t *testing.T) {
	as := assert.New(t)

	s := newGeneratingStream("hello")
	as.NotNil(s)
}

func TestStreamStart(t *testing.T) {
	as := assert.New(t)

	s := newGeneratingStream("hello").Start()
	as.NotNil(s)
	as.Nil(s.Stop())
}

func TestStreamStop(t *testing.T) {
	as := assert.New(t)

	s := newGeneratingStream("hello").Start()
	as.NotNil(s)
	as.Nil(s.Stop())
	as.ErrorIs(s.Stop(), stream.ErrAlreadyStopped)
}

func TestStreamMonitorStop(t *testing.T) {
	as := assert.New(t)
	var s stream.Running
	source := node.Generate(func() (any, bool) {
		return "hello", true
	})
	processor := func(c *context.Context[any, any]) {
		as.True(s.IsRunning())
		c.Advise(context.Stop{})
	}
	s = _stream.New(_stream.Bind(source, processor)).Start()

	as.NotNil(s)
	done := make(chan bool)
	go func() {
		time.Sleep(50 * time.Millisecond)
		as.ErrorIs(s.Stop(), stream.ErrAlreadyStopped)
		done <- true
	}()
	<-done
}

func TestStreamRecoverableError(t *testing.T) {
	as := assert.New(t)
	var s stream.Running
	source := node.Generate(func() (any, bool) {
		return "hello", true
	})
	processor := func(c *context.Context[any, any]) {
		for {
			select {
			case <-c.Done:
				return
			case <-c.In:
				as.True(s.IsRunning())
				c.Errorf("recoverable")
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	s = _stream.New(_stream.Bind(source, processor)).StartWith(
		func(a context.Advice, next func()) {
			e, ok := a.(*context.Error)
			as.True(ok)
			as.EqualError(e, "recoverable")
			next()
		},
	)

	as.NotNil(s)
	done := make(chan bool)
	go func() {
		time.Sleep(50 * time.Millisecond)
		as.True(s.IsRunning())
		_ = s.Stop()
		done <- true
	}()
	<-done
}

func TestStreamFatalError(t *testing.T) {
	as := assert.New(t)
	var s stream.Running
	source := node.Generate(func() (any, bool) {
		return "hello", true
	})
	processor := func(c *context.Context[any, any]) {
		as.True(s.IsRunning())
		c.Fatalf("boom")
	}
	s = _stream.New(_stream.Bind(source, processor)).Start()

	as.NotNil(s)
	done := make(chan bool)
	go func() {
		time.Sleep(50 * time.Millisecond)
		as.False(s.IsRunning())
		done <- true
	}()
	<-done
}
