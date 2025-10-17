package caravan

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/node"

	internal "github.com/kode4food/caravan/internal/stream"
)

// NewStream instantiates a new stream, given a set of Processors
func NewStream[Msg any](
	source stream.Processor[stream.Source, Msg],
	rest ...stream.Processor[Msg, Msg],
) stream.Stream {
	return internal.Make(source, node.Subprocess(rest...))
}
