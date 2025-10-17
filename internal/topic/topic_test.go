package topic_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/topic/config"

	internal "github.com/kode4food/caravan/internal/topic"
)

func TestMakeTopicError(t *testing.T) {
	as := assert.New(t)
	defer func() {
		as.Error(recover().(error))
	}()
	internal.Make[any](config.Permanent, config.Consumed)
}

func TestLongLog(t *testing.T) {
	as := assert.New(t)

	l := internal.Make[any](config.Permanent)
	p := l.NewProducer()
	defer p.Close()

	c := l.NewConsumer()
	defer c.Close()

	go func() {
		for i := 0; i < 10000; i++ {
			p.Send() <- i
		}
	}()

	for i := 0; i < 10000; i++ {
		e := <-c.Receive()
		as.Equal(i, e)
	}

	as.Equal(uint64(10000), l.Length())
}

func TestConsumerReadsAllMessages(t *testing.T) {
	as := assert.New(t)

	l := internal.Make[any](config.Permanent)
	p := l.NewProducer()
	defer p.Close()
	for i := 0; i < 100; i++ {
		p.Send() <- i
	}

	c := l.NewConsumer()
	defer c.Close()
	for i := 0; i < 100; i++ {
		e := <-c.Receive()
		as.Equal(i, e)
	}
}

func TestLogDiscarding(t *testing.T) {
	as := assert.New(t)

	segmentSize := config.DefaultSegmentIncrement
	l := internal.Make[any](config.Consumed)
	p := l.NewProducer()
	defer p.Close()

	c := l.NewConsumer()
	defer c.Close()

	for i := 0; i < segmentSize+3; i++ {
		p.Send() <- i
	}

	for i := 0; i < segmentSize; i++ {
		e := <-c.Receive()
		as.Equal(i, e)
	}

	time.Sleep(10 * time.Millisecond)

	for i := segmentSize; i < segmentSize+3; i++ {
		e := <-c.Receive()
		as.Equal(i, e)
	}
}

func TestLogDiscardEverything(t *testing.T) {
	as := assert.New(t)

	segmentSize := config.DefaultSegmentIncrement
	l := internal.Make[any](config.Consumed)
	p := l.NewProducer()
	defer p.Close()

	c := l.NewConsumer()
	defer c.Close()

	for i := 0; i < segmentSize; i++ {
		p.Send() <- i
	}

	for i := 0; i < segmentSize; i++ {
		e := <-c.Receive()
		as.Equal(i, e)
	}

	time.Sleep(10 * time.Millisecond)

	p.Send() <- segmentSize
	e := <-c.Receive()
	as.Equal(segmentSize, e)
}
