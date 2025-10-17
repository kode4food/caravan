package retention_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/topic/config"
	"github.com/kode4food/caravan/topic/retention"
)

func TestPermanentPolicy(t *testing.T) {
	as := assert.New(t)
	p := retention.MakePermanentPolicy()
	as.NotNil(p)
}

func TestPermanent(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any](config.Permanent)
	p := top.NewProducer()

	for i := 0; i < 500; i++ {
		p.Send() <- i
	}

	done := make(chan bool)
	go func() {
		c := top.NewConsumer()
		for i := 0; i < 500; i++ {
			as.Equal(i, <-c.Receive())
		}
		c.Close()
		done <- true
	}()

	<-done
	p.Close()
}
