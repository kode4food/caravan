package caravan

import (
	_stream "github.com/kode4food/caravan/internal/stream"
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/topic"
)

// StreamBranch builds one branch of a split Stream
type StreamBranch[In, Out any] struct {
	processor stream.Processor[In, Out]
}

// Branch starts building a branch for Split
func (p StreamPipeline[Msg]) Branch() StreamBranch[Msg, Msg] {
	return StreamBranch[Msg, Msg]{
		processor: func(c *context.Context[Msg, Msg]) {
			for {
				msg, ok := c.FetchMessage()
				if !ok || !c.ForwardResult(msg) {
					return
				}
			}
		},
	}
}

// Filter filters a Stream branch
func (b StreamBranch[In, Msg]) Filter(
	fn func(Msg) bool,
) StreamBranch[In, Msg] {
	return b.process(node.Filter(fn))
}

// Map transforms messages in a Stream branch
func (b StreamBranch[In, From]) Map[To any](
	fn func(From) To,
) StreamBranch[In, To] {
	return b.process(node.Map(fn))
}

// TopicProducer sends Stream branch messages to a Topic
func (b StreamBranch[In, Msg]) TopicProducer(
	t topic.Topic[Msg],
) StreamBranch[In, Msg] {
	return b.process(node.TopicProducer(t))
}

// Split sends each message through every provided branch
func (p StreamPipeline[Msg]) Split[Out any](
	branches ...StreamBranch[Msg, Out],
) stream.Stream {
	processors := make([]stream.Processor[Msg, Out], len(branches))
	for i, branch := range branches {
		processors[i] = branch.processor
	}
	return p.process(node.Split(processors...)).Build()
}

func (b StreamBranch[In, From]) process[To any](
	processor stream.Processor[From, To],
) StreamBranch[In, To] {
	return StreamBranch[In, To]{
		processor: _stream.Bind(b.processor, processor),
	}
}
