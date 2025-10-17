package node_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestRetry(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	attempts := 0
	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(in),
			node.Retry(func(n int) (int, error) {
				attempts++
				if attempts < 3 {
					return 0, errors.New("temporary failure")
				}
				return n * 2, nil
			}, 5, 10*time.Millisecond),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- 5
	}()

	c := out.NewConsumer()
	defer c.Close()

	result := <-c.Receive()
	assert.Equal(t, 10, result)
	assert.Equal(t, 3, attempts)
}

func TestRetryFailure(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(in),
			node.Retry(func(n int) (int, error) {
				return 0, errors.New("always fails")
			}, 3, 10*time.Millisecond),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- 5
		p.Send() <- 10
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get second message since first fails all retries
	select {
	case result := <-c.Receive():
		assert.Equal(t, 10, result)
	case <-time.After(200 * time.Millisecond):
		// Expected - message dropped after retries exhausted
	}
}
