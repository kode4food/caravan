package node_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestReduce(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(inTopic),
		node.Subprocess(
			node.Reduce(func(prev int, e int) int {
				return prev + e
			}),
			node.TopicProducer(outTopic),
		),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())
	as.Equal(6, <-c.Receive())

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}

func TestReduceFrom(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(inTopic),
		node.Subprocess(
			node.ReduceFrom(func(prev int, e int) int {
				return prev + e
			}, 5),
			node.TopicProducer(outTopic),
		),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3

	c := outTopic.NewConsumer()
	as.Equal(6, <-c.Receive())
	as.Equal(8, <-c.Receive())
	as.Equal(11, <-c.Receive())

	c.Close()
	p.Close()
	as.Nil(s.Stop())
}

func TestScan(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Scan(func(acc, n int) int { return acc + n }),
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

	// Should emit running totals: 1, 3, 6, 10, 15
	expected := []int{1, 3, 6, 10, 15}
	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp, result)
	}
}

func TestScanFrom(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.ScanFrom(func(acc, n int) int { return acc + n }, 10),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := 1; i <= 3; i++ {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should emit running totals starting from 10: 11, 13, 16
	expected := []int{11, 13, 16}
	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp, result)
	}
}
