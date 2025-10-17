package message

import (
	"errors"

	"github.com/kode4food/caravan/closer"
)

type (
	// Sender is a type that is capable of sending a Message via a channel
	Sender[Msg any] interface {
		Send() chan<- Msg
	}

	// ClosingSender is a Sender that is capable of being closed
	ClosingSender[Msg any] interface {
		closer.Closer
		Sender[Msg]
	}
)

// Error messages
const (
	// ErrSenderClosed is raised when MustSend is called on a closed Sender
	ErrSenderClosed = "sender is closed"
)

// Send sends a message to a ClosingSender
func Send[Msg any](s ClosingSender[Msg], m Msg) bool {
	select {
	case <-s.IsClosed():
		return false
	default:
		s.Send() <- m
		return true
	}
}

// MustSend will send to a ClosingSender or panic if it is closed
func MustSend[Msg any](s ClosingSender[Msg], m Msg) {
	if !Send(s, m) {
		panic(errors.New(ErrSenderClosed))
	}
}
