package caravan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
)

func TestSplit(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	pipeline := caravan.TopicConsumer(inTopic)
	s := pipeline.
		Split(
			pipeline.
				Branch().
				Map(func(i int) int { return i + 1 }).
				TopicProducer(outTopic),
			pipeline.
				Branch().
				Map(func(i int) int { return i * 2 }).
				TopicProducer(outTopic),
		).
		Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	c := outTopic.NewConsumer()
	testUnorderedIntResults(t, c.Receive(), 4, 6, 11, 20)
	c.Close()

	as.Nil(s.Stop())
}
