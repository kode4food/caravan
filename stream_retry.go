package caravan

import (
	"time"

	"github.com/kode4food/caravan/internal/stream/node"
)

// Retry retries failed mappings with exponential backoff
func (p StreamPipeline[From]) Retry[To any](
	fn func(From) (To, error), attempts int, backoff time.Duration,
) StreamPipeline[To] {
	return p.process(node.Retry(fn, attempts, backoff))
}
