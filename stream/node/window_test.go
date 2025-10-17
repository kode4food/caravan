package node_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestWindow(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[[]int]()

	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(in),
			node.Window[int](100*time.Millisecond),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		// Send messages over 250ms
		for i := 0; i < 6; i++ {
			p.Send() <- i
			time.Sleep(50 * time.Millisecond)
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get multiple windows
	window1 := <-c.Receive()
	assert.NotEmpty(t, window1)

	window2 := <-c.Receive()
	assert.NotEmpty(t, window2)
}

func TestSlidingWindow(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[[]int]()

	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(in),
			node.SlidingWindow[int](3),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := 0; i < 5; i++ {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// First window when size is reached
	window1 := <-c.Receive()
	assert.Equal(t, []int{0, 1, 2}, window1)

	// Second window slides
	window2 := <-c.Receive()
	assert.Equal(t, []int{1, 2, 3}, window2)

	// Third window slides
	window3 := <-c.Receive()
	assert.Equal(t, []int{2, 3, 4}, window3)
}
