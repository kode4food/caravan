package message_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/message"
)

func TestPoll(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic[any]()
	p := top.NewProducer()
	message.Send(p, "hello")

	c := top.NewConsumer()
	e, ok := message.Poll(c, time.Millisecond)
	as.Equal("hello", e)
	as.True(ok)

	e, ok = message.Poll[any](c, time.Millisecond)
	as.Nil(e)
	as.False(ok)
	c.Close()
}

func TestMustReceive(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic[any]()
	p := top.NewProducer()
	message.Send(p, "hello")

	c := top.NewConsumer()
	as.Equal("hello", message.MustReceive(c))
	c.Close()

	defer func() {
		as.ErrorIs(recover().(error), message.ErrReceiverClosed)
	}()
	message.MustReceive(c)
}
