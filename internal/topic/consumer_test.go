package topic_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/closer"
	"github.com/kode4food/caravan/debug"
	"github.com/kode4food/caravan/message"
	"github.com/kode4food/caravan/topic"
	"github.com/kode4food/caravan/topic/config"
)

func TestConsumerClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	c := top.NewConsumer()

	c.Close()
	as.True(closer.IsClosed(c))

	c.Close()
	as.True(closer.IsClosed(c)) // still closed
}

func TestConsumerGC(t *testing.T) {
	debug.Enable()

	as := assert.New(t)
	top := caravan.NewTopic[any]()
	top.NewConsumer()
	runtime.GC()

	errs := make(chan error)
	go func() {
		debug.WithConsumer(func(c topic.Consumer[error]) {
			e, _ := message.Receive(c)
			errs <- e
		})
	}()
	as.Error(<-errs)
}

func TestEmptyConsumer(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic[any](config.Permanent)
	c := top.NewConsumer()
	e, ok := message.Poll(c, 0)
	as.Nil(e)
	as.False(ok)
	c.Close()
}

func TestSingleConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any](config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	p.Send() <- "first value"
	p.Send() <- "second value"
	p.Send() <- "third value"

	c := top.NewConsumer()
	as.NotNil(c)

	as.Equal("first value", <-c.Receive())
	as.Equal("second value", <-c.Receive())
	as.Equal("third value", <-c.Receive())

	p.Close()
	c.Close()
}

func TestMultiConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any](config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	p.Send() <- "first value"
	p.Send() <- "second value"
	p.Send() <- "third value"

	c1 := top.NewConsumer()
	c2 := top.NewConsumer()

	as.Equal("first value", <-c1.Receive())
	as.Equal("second value", <-c1.Receive())
	as.Equal("first value", <-c2.Receive())
	as.Equal("second value", <-c2.Receive())
	as.Equal("third value", <-c2.Receive())
	as.Equal("third value", <-c1.Receive())

	p.Close()
	c1.Close()
	c2.Close()
}

func TestLoadedConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any](config.Permanent)
	p := top.NewProducer()

	for i := 0; i < 10000; i++ {
		p.Send() <- i
	}

	done := make(chan bool)

	go func() {
		c := top.NewConsumer()
		for i := 0; i < 10000; i++ {
			as.Equal(i, message.MustReceive(c))
		}
		c.Close()
		done <- true
	}()

	<-done
	p.Close()
}

func TestStreamingConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any](config.Consumed)
	p := top.NewProducer()
	c := top.NewConsumer()

	go func() {
		for i := 0; i < 100000; i++ {
			p.Send() <- i
		}
		p.Close()
	}()

	done := make(chan bool)

	go func() {
		for i := 0; i < 100000; i++ {
			as.Equal(i, message.MustReceive(c))
		}
		done <- true
	}()

	<-done
	c.Close()
}

func TestConsumerClosedDuringPoll(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	p := top.NewProducer()
	c := top.NewConsumer()

	go func() {
		time.Sleep(100 * time.Millisecond)
		c.Close()
		p.Send() <- 1
	}()

	e, ok := message.Poll[any](c, time.Second)
	as.Nil(e)
	as.False(ok)
}

func TestConsumerChannel(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any](config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	pc := p.Send()
	pc <- "first value"
	pc <- "second value"
	pc <- "third value"
	p.Close()

	c := top.NewConsumer()
	as.NotNil(c)

	cc := c.Receive()
	as.Equal("first value", <-cc)
	as.Equal("second value", <-cc)
	as.Equal("third value", <-cc)
	c.Close()
}

func TestConsumerChannelClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	c := top.NewConsumer()
	ch := c.Receive()
	c.Close()

	_, ok := <-ch
	as.False(ok)
}
