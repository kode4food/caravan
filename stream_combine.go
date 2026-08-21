package caravan

import (
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/stream"
)

// Pair contains one value from each of two Streams
type Pair[Left, Right any] struct {
	Left  Left
	Right Right
}

// Merge starts a Stream that merges the provided pipelines
func Merge[Msg any](pipelines ...StreamPipeline[Msg]) StreamPipeline[Msg] {
	processors := make([]stream.Processor[stream.Source, Msg], len(pipelines))
	for i, pipeline := range pipelines {
		processors[i] = pipeline.source
	}
	return start(node.Merge(processors...))
}

// Zip starts a Stream by pairing messages from two pipelines
func Zip[Left, Right any](
	left StreamPipeline[Left], right StreamPipeline[Right],
) StreamPipeline[Pair[Left, Right]] {
	return ZipWith(left, right, func(l Left, r Right) Pair[Left, Right] {
		return Pair[Left, Right]{Left: l, Right: r}
	})
}

// ZipWith starts a Stream by combining paired messages from two pipelines
func ZipWith[Left, Right, Out any](
	left StreamPipeline[Left], right StreamPipeline[Right],
	fn func(Left, Right) Out,
) StreamPipeline[Out] {
	return start(node.ZipWith(left.source, right.source, fn))
}

// CombineLatest starts a Stream using the latest values from two pipelines
func CombineLatest[Left, Right, Out any](
	left StreamPipeline[Left], right StreamPipeline[Right],
	fn func(Left, Right) Out,
) StreamPipeline[Out] {
	return start(node.CombineLatest(left.source, right.source, fn))
}

// Join starts a Stream by joining matching messages from two pipelines
func Join[Left, Right, Out any](
	left StreamPipeline[Left], right StreamPipeline[Right],
	pred func(Left, Right) bool, fn func(Left, Right) Out,
) StreamPipeline[Out] {
	return start(node.Join(left.source, right.source, pred, fn))
}
