package node_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestTake(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Take[int](3),
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

	for i := range 3 {
		result := <-c.Receive()
		assert.Equal(t, i, result)
	}
}

func TestTakeWhile(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.TakeWhile(func(n int) bool { return n < 5 }),
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

	for i := range 5 {
		result := <-c.Receive()
		assert.Equal(t, i, result)
	}
}

func TestSkip(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Skip[int](5),
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

	for i := 5; i < 10; i++ {
		result := <-c.Receive()
		assert.Equal(t, i, result)
	}
}

func TestSkipWhile(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.SkipWhile(func(n int) bool { return n < 5 }),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := range 8 {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	for i := 5; i < 8; i++ {
		result := <-c.Receive()
		assert.Equal(t, i, result)
	}
}

func TestDistinct(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Distinct(func(a, b int) bool { return a == b }),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		values := []int{1, 1, 2, 2, 2, 3, 1}
		for _, v := range values {
			p.Send() <- v
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	expected := []int{1, 2, 3, 1}
	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp, result)
	}
}

func TestDistinctBy(t *testing.T) {
	type Person struct {
		Name string
		ID   int
	}

	in := caravan.NewTopic[Person]()
	out := caravan.NewTopic[Person]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.DistinctBy(func(p Person) int { return p.ID }),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- Person{ID: 1, Name: "Alice"}
		p.Send() <- Person{ID: 1, Name: "Alice2"}
		p.Send() <- Person{ID: 2, Name: "Bob"}
		p.Send() <- Person{ID: 2, Name: "Bob2"}
	}()

	c := out.NewConsumer()
	defer c.Close()

	result := <-c.Receive()
	assert.Equal(t, 1, result.ID)

	result = <-c.Receive()
	assert.Equal(t, 2, result.ID)
}
