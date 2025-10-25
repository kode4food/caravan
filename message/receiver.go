package message

import (
	"errors"
	"time"

	"github.com/kode4food/caravan/closer"
)

type (
	// Receiver is a type that is capable of receiving a Message via a channel
	Receiver[Msg any] interface {
		Receive() <-chan Msg
	}

	// ClosingReceiver is a Receiver that is capable of being closed
	ClosingReceiver[Msg any] interface {
		closer.Closer
		Receiver[Msg]
	}
)

var (
	ErrReceiverClosed = errors.New("receiver closed")
)

// Poll will wait up until the specified Duration for a message to possibly be
// returned, advancing the Receiver's Cursor upon success
func Poll[Msg any](r Receiver[Msg], d time.Duration) (Msg, bool) {
	select {
	case <-time.After(d):
		var zero Msg
		return zero, false
	case m, ok := <-r.Receive():
		return m, ok
	}
}

// Receive returns the next message, blocking indefinitely, and advancing the
// Receiver's Cursor upon completion
func Receive[Msg any](r Receiver[Msg]) (Msg, bool) {
	m, ok := <-r.Receive()
	return m, ok
}

// MustReceive will receive from a Receiver or panic if it is closed
func MustReceive[Msg any](r Receiver[Msg]) Msg {
	if m, ok := Receive(r); ok {
		return m
	}
	panic(ErrReceiverClosed)
}
