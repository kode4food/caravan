package node_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/stream/node"
)

func TestSinkInto(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan int)
	out := make(chan stream.Sink)

	sinkCh := make(chan int)

	sink := node.SinkInto(sinkCh)
	sink.Start(context.Make(done, make(chan context.Advice), in, out))

	go func() {
		in <- 42
		in <- 96
	}()

	go func() {
		as.Equal(42, <-sinkCh)
		as.Equal(96, <-sinkCh)
		close(done)
	}()

	<-done
}
