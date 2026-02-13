package node_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestThrottle(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Throttle[int](100*time.Millisecond),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	start := time.Now()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := range 3 {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	for i := range 3 {
		result := <-c.Receive()
		assert.Equal(t, i, result)
	}

	elapsed := time.Since(start)
	assert.True(t, elapsed >= 200*time.Millisecond)
}

func TestDebounce(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Debounce[int](100*time.Millisecond),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		// Send messages rapidly
		p.Send() <- 1
		time.Sleep(20 * time.Millisecond)
		p.Send() <- 2
		time.Sleep(20 * time.Millisecond)
		p.Send() <- 3
		// Wait for debounce to emit last message
		time.Sleep(150 * time.Millisecond)
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should only get the last message (3) after debounce period
	timeout := time.After(500 * time.Millisecond)
	select {
	case result := <-c.Receive():
		assert.Equal(t, 3, result)
	case <-timeout:
		t.Fatal("timeout waiting for debounced message")
	}
}

func TestDebounceMultipleBursts(t *testing.T) {
	in := caravan.NewTopic[string]()
	out := caravan.NewTopic[string]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Debounce[string](50*time.Millisecond),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()

		// First burst
		p.Send() <- "a"
		time.Sleep(10 * time.Millisecond)
		p.Send() <- "b"
		time.Sleep(10 * time.Millisecond)
		p.Send() <- "c"

		// Wait for debounce
		time.Sleep(100 * time.Millisecond)

		// Second burst
		p.Send() <- "x"
		time.Sleep(10 * time.Millisecond)
		p.Send() <- "y"
		time.Sleep(10 * time.Millisecond)
		p.Send() <- "z"

		// Wait for debounce
		time.Sleep(100 * time.Millisecond)
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get "c" from first burst
	timeout := time.After(200 * time.Millisecond)
	select {
	case result := <-c.Receive():
		assert.Equal(t, "c", result)
	case <-timeout:
		t.Fatal("timeout waiting for first debounced message")
	}

	// Should get "z" from second burst
	timeout = time.After(200 * time.Millisecond)
	select {
	case result := <-c.Receive():
		assert.Equal(t, "z", result)
	case <-timeout:
		t.Fatal("timeout waiting for second debounced message")
	}
}

func TestDebounceSingleMessage(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Debounce[int](50*time.Millisecond),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- 42
		time.Sleep(100 * time.Millisecond)
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get the single message after debounce period
	timeout := time.After(200 * time.Millisecond)
	select {
	case result := <-c.Receive():
		assert.Equal(t, 42, result)
	case <-timeout:
		t.Fatal("timeout waiting for debounced message")
	}
}

func TestDelay(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Delay[int](100*time.Millisecond),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := range 3 {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	start := time.Now()
	for i := range 3 {
		result := <-c.Receive()
		assert.Equal(t, i, result)
	}
	elapsed := time.Since(start)

	// Should take at least 300ms for 3 messages with 100ms delay each
	assert.GreaterOrEqual(t, elapsed, 300*time.Millisecond)
}

func TestSample(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(in),
		node.Sample[int](100*time.Millisecond),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		// Send messages rapidly
		for i := range 10 {
			p.Send() <- i
			time.Sleep(20 * time.Millisecond)
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get sampled messages (roughly every 100ms)
	// With 20ms spacing and 100ms sampling, we should get ~2-3 samples
	var samples []int
	timeout := time.After(500 * time.Millisecond)

	for {
		select {
		case result := <-c.Receive():
			samples = append(samples, result)
		case <-timeout:
			// Should have received some samples but not all 10
			assert.Greater(t, len(samples), 0)
			assert.Less(t, len(samples), 10)
			return
		}
	}
}
