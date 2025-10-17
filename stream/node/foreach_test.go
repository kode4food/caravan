package node_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"

	internal "github.com/kode4food/caravan/internal/stream"
)

func TestForEach(t *testing.T) {
	as := assert.New(t)

	sum := 0
	inTopic := caravan.NewTopic[int]()

	s := internal.Make(
		node.TopicConsumer(inTopic),
		node.ForEach(func(m int) {
			sum += m
		}),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2
	p.Send() <- 3
	p.Close()

	time.Sleep(50 * time.Millisecond)
	as.Equal(6, sum)
	as.Nil(s.Stop())
}
