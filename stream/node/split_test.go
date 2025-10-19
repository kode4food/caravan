package node_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
)

func TestSplit(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	s := caravan.BasicStream(
		node.TopicConsumer(inTopic),
		node.Split(
			node.Bind(
				node.Map(func(i int) int {
					return i + 1
				}),
				node.TopicProducer(outTopic),
			),
			node.Bind(
				node.Map(func(i int) int {
					return i * 2
				}),
				node.TopicProducer(outTopic),
			),
		),
	).Start()

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
