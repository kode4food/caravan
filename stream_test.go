package caravan_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
)

type pipelineOrder struct {
	Amount int
}

func TestStreamPipeline(t *testing.T) {
	in := caravan.NewTopic[*pipelineOrder]()
	out := caravan.NewTopic[int]()
	s := caravan.TopicConsumer(in).
		Filter(func(o *pipelineOrder) bool { return o.Amount > 100 }).
		Map(func(o *pipelineOrder) int { return o.Amount }).
		TopicProducer(out)

	running := s.Start()
	defer running.Stop()
	assert.True(t, running.IsRunning())

	p := in.NewProducer()
	defer p.Close()
	p.Send() <- &pipelineOrder{Amount: 100}
	p.Send() <- &pipelineOrder{Amount: 101}

	c := out.NewConsumer()
	defer c.Close()
	select {
	case amount := <-c.Receive():
		assert.Equal(t, 101, amount)
	case <-time.After(time.Second):
		assert.Fail(t, "timeout")
	}
}

func TestStreamIntegration(t *testing.T) {
	counter := 0
	s := caravan.Generate(func() (int, bool) {
		if counter < 5 {
			counter++
			return counter, true
		}
		return 0, false
	}).
		Filter(func(v int) bool { return v%2 == 0 }).
		Build()
	assert.NotNil(t, s)

	running := s.Start()
	assert.True(t, running.IsRunning())
	assert.NoError(t, running.Stop())
}
