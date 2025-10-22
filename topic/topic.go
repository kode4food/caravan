package topic

import "github.com/kode4food/caravan/message"

type (
	// Topic is where you put your stuff. They are implemented as a
	// first-in-first-out (FIFO) Log.
	Topic[Msg any] interface {
		// Length returns the current virtual size of the Topic
		Length() uint64

		// NewProducer returns a new Producer for this Topic
		NewProducer() Producer[Msg]

		// NewConsumer returns a new Consumer for this Topic
		NewConsumer() Consumer[Msg]
	}

	// Producer exposes a way to push messages to its associated Topic.
	// messages pushed to the Topic are capable of being independently
	// received by all Consumers
	Producer[Msg any] message.ClosingSender[Msg]

	// Consumer exposes a way to receive messages from its associated Topic.
	// Each Consumer created independently tracks its own position within
	// the Topic
	Consumer[Msg any] message.ClosingReceiver[Msg]
)
