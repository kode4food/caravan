package caravan

import (
	"github.com/kode4food/caravan/topic"
	"github.com/kode4food/caravan/topic/config"

	internal "github.com/kode4food/caravan/internal/topic"
)

// NewTopic instantiates a new Topic
func NewTopic[Msg any](o ...config.Option) topic.Topic[Msg] {
	return internal.Make[Msg](o...)
}
