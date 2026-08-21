package caravan

import (
	_topic "github.com/kode4food/caravan/internal/topic"
	"github.com/kode4food/caravan/topic"
)

// NewTopic returns an empty Topic
func NewTopic[Msg any]() topic.Topic[Msg] {
	return _topic.New[Msg]()
}
