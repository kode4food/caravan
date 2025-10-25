package stream

import (
	"log"
	"sync"

	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/stream/node"
)

type (
	// Stream is the internal implementation of a stream.Stream
	Stream[In, Out any] struct {
		root stream.Processor[stream.Source, Out]
	}

	// Running is the internal implementation of a stream.Running
	Running[In, Out any] struct {
		*Stream[In, Out]
		monitor chan context.Advice
		done    chan context.Done
		mu      sync.Mutex
	}
)

// Make builds a Stream. The Stream must be started using the Start method
func Make[In, Out any](
	source stream.Processor[stream.Source, In], rest stream.Processor[In, Out],
) stream.Stream {
	return &Stream[In, Out]{
		root: node.Bind(source, rest),
	}
}

// Start kicks off the background routine for this stream
func (s *Stream[_, _]) Start() stream.Running {
	r := s.start()
	r.startMonitoringWith(r.handleAdvice)
	return r
}

func (s *Stream[_, _]) StartWith(h stream.AdviceHandler) stream.Running {
	r := s.start()
	r.startMonitoringWith(h)
	return r
}

func (s *Stream[In, Out]) start() *Running[In, Out] {
	r := &Running[In, Out]{
		Stream:  s,
		monitor: make(chan context.Advice),
		done:    make(chan context.Done),
	}
	r.startStream()
	return r
}

func (r *Running[_, Out]) startStream() {
	go func() {
		in := make(chan stream.Source)
		out := make(chan stream.Sink)

		loop := node.Bind(
			r.root,
			node.Sink[Out](),
		)

		loop.Start(context.Make(r.done, r.monitor, in, out))

		for {
			select {
			case <-r.done:
				return
			case in <- stream.Source{}:
			}
		}
	}()
}

func (r *Running[_, _]) startMonitoringWith(handle stream.AdviceHandler) {
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

func (r *Running[_, _]) handleAdvice(a context.Advice, _ func()) {
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
func (r *Running[_, _]) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isRunning()
}

func (r *Running[_, _]) isRunning() bool {
	select {
	case <-r.done:
		return false
	default:
		return true
	}
}

// Stop the stream if it's running
func (r *Running[_, _]) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isRunning() {
		return stream.ErrAlreadyStopped
	}
	r.stop()
	return nil
}

func (r *Running[_, _]) stop() {
	close(r.done)
}
