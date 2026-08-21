package node

import (
	"github.com/kode4food/caravan/topic"

	"github.com/kode4food/caravan/stream"
)

// TopicConsumer constructs a processor that receives from the provided Topic
// every time it's invoked by the Stream
func TopicConsumer[Msg any](
	t topic.Topic[Msg],
) stream.Processor[stream.Source, Msg] {
	ch := t.NewConsumer().Receive()
	return GenerateFrom(ch)
}

// TopicProducer constructs a processor that sends all messages it sees to the
// provided Topic
func TopicProducer[Msg any](t topic.Topic[Msg]) stream.Processor[Msg, Msg] {
	ch := t.NewProducer().Send()
	return SidechainTo(ch)
}
