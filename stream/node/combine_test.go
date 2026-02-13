package node_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/message"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/stream/node"
)

func TestEmptySubprocess(t *testing.T) {
	as := assert.New(t)

	s := node.Subprocess[any]()
	as.NotNil(s)

	done := make(chan context.Done)
	in := make(chan any)
	out := make(chan any)

	s.Start(context.Make(done, make(chan context.Advice), in, out))
	in <- "hello"
	as.Equal("hello", <-out)
	close(done)
}

func TestSingleSubprocess(t *testing.T) {
	as := assert.New(t)

	double := node.Subprocess(func(c *context.Context[int, int]) {
		c.Out <- (<-c.In) * 2
	})

	as.NotNil(double)

	done := make(chan context.Done)
	in := make(chan int)
	out := make(chan int)

	double.Start(context.Make(done, make(chan context.Advice), in, out))
	in <- 8
	as.Equal(16, <-out)
	close(done)
}

func TestDoubleSubprocess(t *testing.T) {
	as := assert.New(t)

	s := node.Subprocess(
		node.Forward[any],
		func(c *context.Context[any, any]) {
			<-c.In
			c.Errorf("explosion")
			<-c.Done
		},
	)

	done := make(chan context.Done)
	monitor := make(chan context.Advice)
	in := make(chan any)

	s.Start(context.Make(done, monitor, in, make(chan any)))
	in <- "anything"
	as.EqualError((<-monitor).(error), "explosion")
	close(done)
}

func TestSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[string]()
	outTopic := caravan.NewTopic[string]()

	s := caravan.NewStream(
		node.TopicConsumer(inTopic),
		node.TopicProducer(outTopic),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- "hello"
	p.Close()

	c := outTopic.NewConsumer()
	as.Equal("hello", <-c.Receive())
	c.Close()

	as.Nil(s.Stop())
}

func TestStatefulSubprocess(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.TopicConsumer(inTopic),
		node.Subprocess(
			node.Forward[int],
			node.Reduce(func(l int, r int) int {
				return l + r
			}),
			node.TopicProducer(outTopic),
		),
	).Start()

	p := inTopic.NewProducer()
	p.Send() <- 1
	p.Send() <- 2

	time.Sleep(50 * time.Millisecond)
	c := outTopic.NewConsumer()
	as.Equal(3, <-c.Receive())

	p.Close()
	c.Close()
	as.Nil(s.Stop())
}

// Tests for Merge

func TestMerge(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.Merge(
			node.Bind(
				node.TopicConsumer(inTopic),
				node.Map(func(i int) int {
					return i + 1
				}),
			),
			node.Bind(
				node.TopicConsumer(inTopic),
				node.Map(func(i int) int {
					return i * 2
				}),
			),
		),
		node.TopicProducer(outTopic),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- 3
	p.Send() <- 10
	p.Close()

	c := outTopic.NewConsumer()
	testUnorderedIntResults(t, c.Receive(), 4, 6, 11, 20)
	c.Close()

	as.Nil(s.Stop())
}

func testUnorderedIntResults(t *testing.T, c <-chan int, nums ...int) {
	as := assert.New(t)

	choices := map[int]bool{}
	for _, n := range nums {
		choices[n] = true
	}
	for range nums {
		v := <-c
		_, ok := choices[v]
		as.True(ok)
		delete(choices, v)
	}

	as.Zero(len(choices))
}

// Tests for Zip

func TestZip(t *testing.T) {
	left := caravan.NewTopic[int]()
	right := caravan.NewTopic[string]()
	out := caravan.NewTopic[node.Pair[int, string]]()

	s := caravan.NewStream(
		node.Zip(
			node.TopicConsumer(left),
			node.TopicConsumer(right),
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		lp := left.NewProducer()
		defer lp.Close()
		for i := 1; i <= 3; i++ {
			lp.Send() <- i
		}
	}()

	go func() {
		rp := right.NewProducer()
		defer rp.Close()
		rp.Send() <- "a"
		rp.Send() <- "b"
		rp.Send() <- "c"
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get paired results
	result1 := <-c.Receive()
	assert.Equal(t, 1, result1.Left)
	assert.Equal(t, "a", result1.Right)

	result2 := <-c.Receive()
	assert.Equal(t, 2, result2.Left)
	assert.Equal(t, "b", result2.Right)

	result3 := <-c.Receive()
	assert.Equal(t, 3, result3.Left)
	assert.Equal(t, "c", result3.Right)
}

func TestZipWith(t *testing.T) {
	left := caravan.NewTopic[int]()
	right := caravan.NewTopic[int]()
	out := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.ZipWith(
			node.TopicConsumer(left),
			node.TopicConsumer(right),
			func(l, r int) int { return l + r },
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	go func() {
		lp := left.NewProducer()
		defer lp.Close()
		lp.Send() <- 1
		lp.Send() <- 2
		lp.Send() <- 3
	}()

	go func() {
		rp := right.NewProducer()
		defer rp.Close()
		rp.Send() <- 10
		rp.Send() <- 20
		rp.Send() <- 30
	}()

	c := out.NewConsumer()
	defer c.Close()

	// Should get sums: 11, 22, 33
	expected := []int{11, 22, 33}
	for _, exp := range expected {
		result := <-c.Receive()
		assert.Equal(t, exp, result)
	}
}

// Tests for CombineLatest

func TestCombineLatest(t *testing.T) {
	left := caravan.NewTopic[int]()
	right := caravan.NewTopic[string]()
	out := caravan.NewTopic[string]()

	s := caravan.NewStream(
		node.CombineLatest(
			node.TopicConsumer(left),
			node.TopicConsumer(right),
			func(l int, r string) string {
				return fmt.Sprintf("%d-%s", l, r)
			},
		),
		node.TopicProducer(out),
	)

	running := s.Start()
	defer running.Stop()

	lp := left.NewProducer()
	defer lp.Close()
	rp := right.NewProducer()
	defer rp.Close()

	c := out.NewConsumer()
	defer c.Close()

	// Send initial values
	lp.Send() <- 1
	time.Sleep(10 * time.Millisecond)
	rp.Send() <- "a"
	time.Sleep(10 * time.Millisecond)

	result1 := <-c.Receive()
	assert.Equal(t, "1-a", result1)

	// Update left - should emit new combination with latest right
	lp.Send() <- 2
	time.Sleep(10 * time.Millisecond)

	result2 := <-c.Receive()
	assert.Equal(t, "2-a", result2)

	// Update right - should emit new combination with latest left
	rp.Send() <- "b"
	time.Sleep(10 * time.Millisecond)

	result3 := <-c.Receive()
	assert.Equal(t, "2-b", result3)
}

// Tests for Join

func joinGreaterThan(l int, r int) bool {
	return l > r
}

func joinSum(l int, r int) int {
	return l + r
}

func makeJoinError(e error) stream.Processor[stream.Source, int] {
	return func(c *context.Context[stream.Source, int]) {
		<-c.In
		c.Error(e)
		<-c.Done
	}
}

func TestJoin(t *testing.T) {
	as := assert.New(t)

	leftTopic := caravan.NewTopic[int]()
	rightTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.Join(
			node.TopicConsumer(leftTopic),
			node.TopicConsumer(rightTopic),
			joinGreaterThan, joinSum,
		),
		node.TopicProducer(outTopic),
	).Start()

	as.NotNil(s)
	lp := leftTopic.NewProducer()
	rp := rightTopic.NewProducer()
	lp.Send() <- 3 // no match
	time.Sleep(10 * time.Millisecond)
	rp.Send() <- 10 // no match
	rp.Send() <- 3
	time.Sleep(10 * time.Millisecond)
	lp.Send() <- 5
	rp.Send() <- 4 // no match
	time.Sleep(10 * time.Millisecond)
	lp.Send() <- 3 // no match
	rp.Send() <- 9
	time.Sleep(10 * time.Millisecond)
	lp.Send() <- 12
	rp.Close()
	lp.Close()

	c := outTopic.NewConsumer() // Otherwise it's discarded
	as.Equal(8, <-c.Receive())
	as.Equal(21, <-c.Receive())
	c.Close()

	as.Nil(s.Stop())
}

func TestJoinErrored(t *testing.T) {
	as := assert.New(t)

	inTopic := caravan.NewTopic[int]()
	outTopic := caravan.NewTopic[int]()

	s := caravan.NewStream(
		node.Join(
			node.TopicConsumer(inTopic),
			makeJoinError(errors.New("error")),
			joinGreaterThan, joinSum,
		),
		node.TopicProducer(outTopic),
	).Start()

	as.NotNil(s)
	p := inTopic.NewProducer()
	p.Send() <- 32
	p.Close()

	c := outTopic.NewConsumer()
	e, ok := message.Poll[int](c, 100*time.Millisecond) // nothing should come out
	as.Zero(e)
	as.False(ok)
	c.Close()

	as.Nil(s.Stop())
}
