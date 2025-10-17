package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/topic/config"
)

func TestDefaults(t *testing.T) {
	as := assert.New(t)
	top1 := caravan.NewTopic[any](config.Defaults, config.Defaults)
	as.NotNil(top1)

	top2 := caravan.NewTopic[any](config.Permanent, config.Defaults)
	as.NotNil(top2)

	top3 := caravan.NewTopic[any](config.Consumed, config.Defaults)
	as.NotNil(top3)
}
