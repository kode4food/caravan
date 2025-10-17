package node_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/stream/node"
)

func TestGenerate(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan stream.Source)
	out := make(chan int)

	gen := node.Generate(func() (int, bool) {
		return 42, true
	})

	gen.Start(context.Make(done, make(chan context.Advice), in, out))

	in <- stream.Source{}
	as.Equal(42, <-out)
	close(done)
}

func TestGenerateFrom(t *testing.T) {
	as := assert.New(t)

	done := make(chan context.Done)
	in := make(chan stream.Source)
	out := make(chan int)

	genCh := make(chan int)

	gen := node.GenerateFrom(genCh)
	gen.Start(context.Make(done, make(chan context.Advice), in, out))

	in <- stream.Source{}
	genCh <- 42
	as.Equal(42, <-out)

	in <- stream.Source{}
	genCh <- 96
	as.Equal(96, <-out)

	close(done)
}
