package caravan_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
)

func TestWindow(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[[]int]()

	s := caravan.TopicConsumer(in).
		Window(100 * time.Millisecond).
		TopicProducer(out)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		// Send messages over 250ms
		for i := range 6 {
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

	s := caravan.TopicConsumer(in).
		SlidingWindow(3).
		TopicProducer(out)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := range 5 {
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
