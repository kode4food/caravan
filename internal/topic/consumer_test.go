package topic_test

import (
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/closer"
	testutil "github.com/kode4food/caravan/internal/testing"
	"github.com/kode4food/caravan/message"
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
	as := assert.New(t)

	h := testutil.NewTestSlogHandler()
	oldHandler := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(oldHandler)

	top := caravan.NewTopic[any]()
	top.NewConsumer()
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	select {
	case r := <-h.Logs:
		as.Contains(r.Message, "consumer finalized without being closed")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for debug log")
	}
}

func TestEmptyConsumer(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic[any]()
	c := top.NewConsumer()
	e, ok := message.Poll(c, 0)
	as.Nil(e)
	as.False(ok)
	c.Close()
}

func TestSingleConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
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

	top := caravan.NewTopic[any]()
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

	top := caravan.NewTopic[any]()
	p := top.NewProducer()
	c := top.NewConsumer()

	for i := 0; i < 10000; i++ {
		p.Send() <- i
	}

	for i := 0; i < 10000; i++ {
		as.Equal(i, message.MustReceive(c))
	}

	c.Close()
	p.Close()
}

func TestStreamingConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
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

	top := caravan.NewTopic[any]()
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
