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

func TestProducerClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	p := top.NewProducer()

	p.Close()
	as.True(closer.IsClosed(p))
	as.False(message.Send[any](p, "blah"))

	p.Close()
	as.True(closer.IsClosed(p)) // still closed
}

func TestProducerGC(t *testing.T) {
	as := assert.New(t)

	h := testutil.NewTestSlogHandler()
	oldHandler := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(oldHandler)

	top := caravan.NewTopic[any]()
	top.NewProducer()
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	select {
	case r := <-h.Logs:
		as.Contains(r.Message, "producer not closed")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for debug log")
	}
}

func TestProducer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	as.NotNil(top)

	p := top.NewProducer()
	c := top.NewConsumer()

	as.NotNil(p)

	p.Send() <- "first value"
	p.Send() <- "second value"
	p.Send() <- "third value"

	time.Sleep(10 * time.Millisecond)
	as.Equal(uint64(3), top.Length())
	p.Close()
	c.Close()
}

func TestLateProducer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	p := top.NewProducer()

	pc := p.Send()
	pc <- "first value"

	c := top.NewConsumer()
	cc := c.Receive()
	as.Equal("first value", <-cc)

	done := make(chan bool)

	go func() {
		as.Equal("second value", <-cc)
		c.Close()
		done <- true
	}()

	pc <- "second value"

	<-done
	p.Close()
}

func TestProducerChannel(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	pc := p.Send()
	pc <- "first value"
	pc <- "second value"
	pc <- "third value"

	done := make(chan bool)
	go func() {
		c := top.NewConsumer()
		time.Sleep(10 * time.Millisecond)
		as.Equal(uint64(3), top.Length())
		c.Close()
		done <- true
	}()

	<-done
	p.Close()
}

func TestProducerChannelClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic[any]()
	p := top.NewProducer()
	ch := p.Send()
	p.Close()

	defer func() {
		as.NotNil(recover())
	}()

	ch <- "hello"
}
