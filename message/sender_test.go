package message_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/message"
)

func TestMustSend(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic[any]()
	p := top.NewProducer()
	message.MustSend[any](p, "hello")
	p.Close()

	defer func() {
		as.ErrorIs(recover().(error), message.ErrSenderClosed)
	}()
	message.MustSend(p, "explode")
}
