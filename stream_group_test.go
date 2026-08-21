package caravan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
)

type event struct {
	UserID string
	Action string
	Value  int
}

func TestGroupBy(t *testing.T) {
	in := caravan.NewTopic[event]()
	out := caravan.NewTopic[*caravan.Grouped[event, string]]()

	s := caravan.TopicConsumer(in).
		GroupBy(func(e event) string {
			return e.UserID
		}).
		TopicProducer(out)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- event{UserID: "bob", Action: "click", Value: 2}
		p.Send() <- event{UserID: "alice", Action: "logout", Value: 3}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// First grouped message
	result1 := <-c.Receive()
	assert.Equal(t, "alice", result1.Key)
	assert.Equal(t, "login", result1.Message.Action)
	assert.Equal(t, 1, result1.Message.Value)

	// Second grouped message
	result2 := <-c.Receive()
	assert.Equal(t, "bob", result2.Key)
	assert.Equal(t, "click", result2.Message.Action)
	assert.Equal(t, 2, result2.Message.Value)

	// Third grouped message
	result3 := <-c.Receive()
	assert.Equal(t, "alice", result3.Key)
	assert.Equal(t, "logout", result3.Message.Action)
	assert.Equal(t, 3, result3.Message.Value)
}

func TestGroupedKeyExtractor(t *testing.T) {
	in := caravan.NewTopic[event]()
	out := caravan.NewTopic[string]()

	s := caravan.TopicConsumer(in).
		GroupBy(func(e event) string {
			return e.UserID
		}).
		Map(func(g *caravan.Grouped[event, string]) string { return g.Key }).
		TopicProducer(out)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- event{UserID: "bob", Action: "click", Value: 2}
		p.Send() <- event{UserID: "alice", Action: "logout", Value: 3}
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get just the keys
	assert.Equal(t, "alice", <-c.Receive())
	assert.Equal(t, "bob", <-c.Receive())
	assert.Equal(t, "alice", <-c.Receive())
}

func TestGroupedMessageExtractor(t *testing.T) {
	in := caravan.NewTopic[event]()
	out := caravan.NewTopic[event]()

	s := caravan.TopicConsumer(in).
		GroupBy(func(e event) string {
			return e.UserID
		}).
		Map(func(g *caravan.Grouped[event, string]) event { return g.Message }).
		TopicProducer(out)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- event{UserID: "bob", Action: "click", Value: 2}
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
	out := caravan.NewTopic[*caravan.Grouped[int, int]]()

	s := caravan.TopicConsumer(in).
		GroupBy(
			// Group by even/odd (mod 2)
			func(n int) int { return n % 2 }).TopicProducer(out)

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
		{key: 1, msg: 1}, {key: 0, msg: 2}, {key: 1, msg: 3},
		{key: 0, msg: 4}, {key: 1, msg: 5}, {key: 0, msg: 6},
	}

	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp.key, result.Key)
		assert.Equal(t, exp.msg, result.Message)
	}
}

func TestGroupByWithAggregation(t *testing.T) {
	in := caravan.NewTopic[event]()
	out := caravan.NewTopic[map[string]int]()

	// Group events by user and count actions per user
	s := caravan.TopicConsumer(in).
		GroupBy(func(e event) string {
			return e.UserID
		}).
		ScanFrom(
			func(
				counts map[string]int, g *caravan.Grouped[event, string],
			) map[string]int {
				counts[g.Key]++
				return counts
			},
			make(map[string]int),
		).
		TopicProducer(out)

	running := s.Start()
	defer running.Stop()

	go func() {
		p := in.NewProducer()
		defer p.Close()
		p.Send() <- event{UserID: "alice", Action: "login", Value: 1}
		p.Send() <- event{UserID: "bob", Action: "click", Value: 2}
		p.Send() <- event{UserID: "alice", Action: "logout", Value: 3}
		p.Send() <- event{UserID: "alice", Action: "click", Value: 4}
		p.Send() <- event{UserID: "bob", Action: "logout", Value: 5}
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
