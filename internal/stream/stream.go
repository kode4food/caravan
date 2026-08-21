package stream

import (
	"log"
	"sync"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

type (
	definition[Out any] struct {
		root stream.Processor[stream.Source, Out]
	}

	running[Out any] struct {
		*definition[Out]
		monitor chan context.Advice
		done    chan context.Done
		mu      sync.Mutex
	}
)

// New returns a Stream that has not been started
func New[Out any](root stream.Processor[stream.Source, Out]) stream.Stream {
	return &definition[Out]{root: root}
}

// Bind joins two Processors while keeping composition out of the public API
func Bind[In, Bound, Out any](
	left stream.Processor[In, Bound], right stream.Processor[Bound, Out],
) stream.Processor[In, Out] {
	return func(c *context.Context[In, Out]) {
		handoff := make(chan Bound)
		left.Start(context.WithOut(c, handoff))
		right.Start(context.WithIn(c, handoff))
	}
}

// Start kicks off the background routine for this stream
func (d *definition[_]) Start() stream.Running {
	r := d.start()
	r.startMonitoringWith(r.handleAdvice)
	return r
}

// StartWith starts the Stream using an Advice handler
func (d *definition[_]) StartWith(h stream.AdviceHandler) stream.Running {
	r := d.start()
	r.startMonitoringWith(h)
	return r
}

func (d *definition[Out]) start() *running[Out] {
	r := &running[Out]{
		definition: d,
		monitor:    make(chan context.Advice),
		done:       make(chan context.Done),
	}
	r.startStream()
	return r
}

func (r *running[Out]) startStream() {
	go func() {
		in := make(chan stream.Source)
		out := make(chan stream.Sink)

		loop := Bind(
			r.root,
			func(c *context.Context[Out, stream.Sink]) {
				for {
					if _, ok := c.FetchMessage(); !ok {
						return
					}
				}
			},
		)

		loop.Start(context.New(r.done, r.monitor, in, out))

		for {
			select {
			case <-r.done:
				return
			case in <- stream.Source{}:
			}
		}
	}()
}

func (r *running[_]) startMonitoringWith(handle stream.AdviceHandler) {
	go func() {
		for {
			select {
			case <-r.done:
				return
			case a := <-r.monitor:
				handle(a, func() {
					r.handleAdvice(a, nil)
				})
			}
		}
	}()
}

func (r *running[_]) handleAdvice(a context.Advice, _ func()) {
	switch e := a.(type) {
	case *context.Error:
		log.Print(e.Error())
	case *context.Fatal:
		log.Print(e.Error())
		_ = r.Stop()
	case context.Stop:
		_ = r.Stop()
	}
}

// IsRunning returns whether the stream is actively running
func (r *running[_]) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isRunning()
}

func (r *running[_]) isRunning() bool {
	select {
	case <-r.done:
		return false
	default:
		return true
	}
}

// Stop the stream if it's running
func (r *running[_]) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isRunning() {
		return stream.ErrAlreadyStopped
	}
	close(r.done)
	return nil
}
