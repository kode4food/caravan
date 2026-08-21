package node_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/stream/context"
)

func TestThrottleClose(t *testing.T) {
	processor := node.Throttle[string](20 * time.Millisecond)
	done := make(chan context.Done)
	in := make(chan string, 2)
	out := make(chan string)
	processor.Start(context.New(done, make(chan context.Advice), in, out))

	in <- "first"
	in <- "second"
	select {
	case msg := <-out:
		assert.Equal(t, "first", msg)
	case <-time.After(50 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
	close(done)
}

func TestDebounceClose(t *testing.T) {
	processor := node.Debounce[string](30 * time.Millisecond)
	done := make(chan context.Done)
	in := make(chan string, 3)
	out := make(chan string, 1)
	processor.Start(context.New(done, make(chan context.Advice), in, out))

	in <- "first"
	in <- "second"
	in <- "third"
	close(in)
	time.Sleep(50 * time.Millisecond)
	close(done)
}

func TestDelayClose(t *testing.T) {
	processor := node.Delay[string](20 * time.Millisecond)
	done := make(chan context.Done)
	in := make(chan string, 2)
	out := make(chan string)
	processor.Start(context.New(done, make(chan context.Advice), in, out))

	in <- "first"
	select {
	case msg := <-out:
		assert.Equal(t, "first", msg)
	case <-time.After(50 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
	in <- "second"
	close(done)
}

func TestSampleClose(t *testing.T) {
	processor := node.Sample[string](30 * time.Millisecond)
	done := make(chan context.Done)
	in := make(chan string, 3)
	out := make(chan string, 3)
	processor.Start(context.New(done, make(chan context.Advice), in, out))

	in <- "first"
	time.Sleep(5 * time.Millisecond)
	in <- "second"
	time.Sleep(5 * time.Millisecond)
	in <- "third"
	time.Sleep(40 * time.Millisecond)
	close(done)
}
