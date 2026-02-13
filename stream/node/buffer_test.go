package node_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestBuffer(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[[]int]()

	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(in),
			node.Buffer[int](3, 1*time.Second),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		for i := range 6 {
			p.Send() <- i
		}
		p.Close()
	}()

	c := out.NewConsumer()
	defer c.Close()

	batch1 := <-c.Receive()
	assert.Equal(t, []int{0, 1, 2}, batch1)

	batch2 := <-c.Receive()
	assert.Equal(t, []int{3, 4, 5}, batch2)
}
