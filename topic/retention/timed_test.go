package retention_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/topic/config"
	"github.com/kode4food/caravan/topic/retention"
)

func TestTimedPolicy(t *testing.T) {
	as := assert.New(t)
	p := retention.MakeTimedPolicy(144 * time.Millisecond)
	as.NotNil(p)
	as.Equal(time.Millisecond*144, p.Duration())
}

func TestTimed(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic[any](config.Timed(50 * time.Millisecond))
	segmentSize := config.DefaultSegmentIncrement
	p := top.NewProducer()
	c := top.NewConsumer()

	for i := 0; i < segmentSize; i++ {
		p.Send() <- i
	}

	time.Sleep(100 * time.Millisecond)

	for i := segmentSize; i < segmentSize*2; i++ {
		p.Send() <- i
	}

	as.Equal(segmentSize, <-c.Receive())
	p.Close()
	c.Close()
}
