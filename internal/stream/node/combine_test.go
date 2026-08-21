package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	_stream "github.com/kode4food/caravan/internal/stream"
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/message"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

func TestJoinError(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()
	joined := node.Join(
		node.TopicConsumer(in), joinErrorProcessor(errors.New("error")),
		func(l, r int) bool { return l > r },
		func(l, r int) int { return l + r },
	)
	root := _stream.Bind(joined, node.TopicProducer(out))
	running := _stream.New(root).Start()
	defer running.Stop()

	p := in.NewProducer()
	p.Send() <- 32
	p.Close()

	c := out.NewConsumer()
	defer c.Close()
	msg, ok := message.Poll(c, 100*time.Millisecond)
	assert.Zero(t, msg)
	assert.False(t, ok)
}

func joinErrorProcessor(err error) stream.Processor[stream.Source, int] {
	return func(c *context.Context[stream.Source, int]) {
		<-c.In
		c.Error(err)
		<-c.Done
	}
}
