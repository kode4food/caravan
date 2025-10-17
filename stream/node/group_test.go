package node_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

type Event struct {
	UserID string
	Action string
	Value  int
}

func TestGroupBy(t *testing.T) {
	in := caravan.NewTopic[Event]()
	out := caravan.NewTopic[*node.Grouped[Event, string]]()

	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(in),
			node.GroupBy(func(e Event) string { return e.UserID }),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- Event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- Event{UserID: "bob", Action: "click", Value: 2}
		p.Send() <- Event{UserID: "alice", Action: "logout", Value: 3}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// First grouped message
	result1 := <-c.Receive()
	assert.Equal(t, "alice", result1.Key())
	assert.Equal(t, "login", result1.Message().Action)
	assert.Equal(t, 1, result1.Message().Value)

	// Second grouped message
	result2 := <-c.Receive()
	assert.Equal(t, "bob", result2.Key())
	assert.Equal(t, "click", result2.Message().Action)
	assert.Equal(t, 2, result2.Message().Value)

	// Third grouped message
	result3 := <-c.Receive()
	assert.Equal(t, "alice", result3.Key())
	assert.Equal(t, "logout", result3.Message().Action)
	assert.Equal(t, 3, result3.Message().Value)
}

func TestGroupedKeyExtractor(t *testing.T) {
	in := caravan.NewTopic[Event]()
	out := caravan.NewTopic[string]()

	s := caravan.NewStream(
		node.Bind(
			node.Bind(
				node.TopicConsumer(in),
				node.GroupBy(func(e Event) string { return e.UserID }),
			),
			node.Map(node.GroupedKey[Event, string]),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- Event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- Event{UserID: "bob", Action: "click", Value: 2}
		p.Send() <- Event{UserID: "alice", Action: "logout", Value: 3}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get just the keys
	assert.Equal(t, "alice", <-c.Receive())
	assert.Equal(t, "bob", <-c.Receive())
	assert.Equal(t, "alice", <-c.Receive())
}

func TestGroupedMessageExtractor(t *testing.T) {
	in := caravan.NewTopic[Event]()
	out := caravan.NewTopic[Event]()

	s := caravan.NewStream(
		node.Bind(
			node.Bind(
				node.TopicConsumer(in),
				node.GroupBy(func(e Event) string { return e.UserID }),
			),
			node.Map(node.GroupedMessage[Event, string]),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- Event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- Event{UserID: "bob", Action: "click", Value: 2}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get the original messages back
	event1 := <-c.Receive()
	assert.Equal(t, "alice", event1.UserID)
	assert.Equal(t, "login", event1.Action)

	event2 := <-c.Receive()
	assert.Equal(t, "bob", event2.UserID)
	assert.Equal(t, "click", event2.Action)
}

func TestGroupByWithIntKey(t *testing.T) {
	in := caravan.NewTopic[int]()
	out := caravan.NewTopic[*node.Grouped[int, int]]()

	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(in),
			// Group by even/odd (mod 2)
			node.GroupBy(func(n int) int { return n % 2 }),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		for i := 1; i <= 6; i++ {
			p.Send() <- i
		}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Verify grouping: 1,2,3,4,5,6 -> keys: 1,0,1,0,1,0
	expected := []struct {
		key int
		msg int
	}{
		{1, 1}, {0, 2}, {1, 3}, {0, 4}, {1, 5}, {0, 6},
	}

	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp.key, result.Key())
		assert.Equal(t, exp.msg, result.Message())
	}
}

func TestGroupByWithAggregation(t *testing.T) {
	in := caravan.NewTopic[Event]()
	out := caravan.NewTopic[map[string]int]()

	// Group events by user and count actions per user
	s := caravan.NewStream(
		node.Bind(
			node.Bind(
				node.TopicConsumer(in),
				node.GroupBy(func(e Event) string { return e.UserID }),
			),
			node.ScanFrom(
				func(
					counts map[string]int, g *node.Grouped[Event, string],
				) map[string]int {
					counts[g.Key()]++
					return counts
				},
				make(map[string]int),
			),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- Event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- Event{UserID: "bob", Action: "click", Value: 2}
		p.Send() <- Event{UserID: "alice", Action: "logout", Value: 3}
		p.Send() <- Event{UserID: "alice", Action: "click", Value: 4}
		p.Send() <- Event{UserID: "bob", Action: "logout", Value: 5}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Collect all results
	var lastCounts map[string]int
	receivedCount := 0

	for receivedCount < 5 {
		result := <-c.Receive()
		lastCounts = result
		receivedCount++
	}

	// Final counts should be: alice=3, bob=2
	assert.Equal(t, 3, lastCounts["alice"])
	assert.Equal(t, 2, lastCounts["bob"])
}
