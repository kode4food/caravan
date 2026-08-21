package caravan

import (
	"time"

	"github.com/kode4food/caravan/internal/stream/node"
)

// Throttle limits message emission to the provided rate
func (p StreamPipeline[Msg]) Throttle(rate time.Duration) StreamPipeline[Msg] {
	return p.process(node.Throttle[Msg](rate))
}

// Debounce emits after the provided quiet period
func (p StreamPipeline[Msg]) Debounce(d time.Duration) StreamPipeline[Msg] {
	return p.process(node.Debounce[Msg](d))
}

// Delay delays each message by the provided duration
func (p StreamPipeline[Msg]) Delay(d time.Duration) StreamPipeline[Msg] {
	return p.process(node.Delay[Msg](d))
}

// Sample emits at most one message per period
func (p StreamPipeline[Msg]) Sample(period time.Duration) StreamPipeline[Msg] {
	return p.process(node.Sample[Msg](period))
}

// SidechainTo forwards messages to a channel and downstream
func (p StreamPipeline[Msg]) SidechainTo(ch chan<- Msg) StreamPipeline[Msg] {
	return p.process(node.SidechainTo(ch))
}
