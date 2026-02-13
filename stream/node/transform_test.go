package node_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestFilter(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Filter(func(n int) bool { return n%2 == 0 }),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := range 10 {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should only get even numbers: 0, 2, 4, 6, 8
	expected := []int{0, 2, 4, 6, 8}
	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp, result)
	}
}

func TestMap(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Map(func(n int) int { return n * 2 }),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := 1; i <= 5; i++ {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get doubled values: 2, 4, 6, 8, 10
	expected := []int{2, 4, 6, 8, 10}
	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp, result)
	}
}

func TestFlatMap(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.FlatMap(func(n int) []int {
			// Return n copies of n (e.g., 3 -> [3, 3, 3])
			result := make([]int, n)
			for i := range result {
				result[i] = n
			}
			return result
		}),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- 1
		p.Send() <- 2
		p.Send() <- 3
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get: 1, 2, 2, 3, 3, 3
	expected := []int{1, 2, 2, 3, 3, 3}
	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp, result)
	}
}
