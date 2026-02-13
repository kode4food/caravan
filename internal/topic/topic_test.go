package topic_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
)

func TestLongLog(t *testing.T) {
	as := assert.New(t)

	l := caravan.NewTopic[any]()
	p := l.NewProducer()
	defer p.Close()

	c := l.NewConsumer()
	defer c.Close()

	go func() {
		for i := range 10000 {
			p.Send() <- i
		}
	}()

	for i := range 10000 {
		e := <-c.Receive()
		as.Equal(i, e)
	}

	as.Equal(uint64(10000), l.Length())
}

func TestConsumerReadsAllMessages(t *testing.T) {
	as := assert.New(t)

	l := caravan.NewTopic[any]()
	p := l.NewProducer()
	defer p.Close()
	for i := range 100 {
		p.Send() <- i
	}

	c := l.NewConsumer()
	defer c.Close()
	for i := range 100 {
		e := <-c.Receive()
		as.Equal(i, e)
	}
}

func TestLogDiscarding(t *testing.T) {
	as := assert.New(t)

	segmentSize := 256
	l := caravan.NewTopic[any]()
	p := l.NewProducer()
	defer p.Close()

	c := l.NewConsumer()
	defer c.Close()

	for i := 0; i < segmentSize+3; i++ {
		p.Send() <- i
	}

	for i := range segmentSize {
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

	segmentSize := 256
	l := caravan.NewTopic[any]()
	p := l.NewProducer()
	defer p.Close()

	c := l.NewConsumer()
	defer c.Close()

	for i := range segmentSize {
		p.Send() <- i
	}

	for i := range segmentSize {
		e := <-c.Receive()
		as.Equal(i, e)
	}

	time.Sleep(10 * time.Millisecond)

	p.Send() <- segmentSize
	e := <-c.Receive()
	as.Equal(segmentSize, e)
}
