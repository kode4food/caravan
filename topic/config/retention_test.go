package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/topic/config"
	"github.com/kode4food/caravan/topic/retention"
)

type explodingRetentionPolicy bool

const errRetentionExplosion = "intentional retention explosion"

func TestRetentionConflict(t *testing.T) {
	as := assert.New(t)

	defer func() {
		rec := recover()
		as.NotNil(rec)
		as.Errorf(rec.(error), config.ErrRetentionPolicyAlreadySet)
	}()

	caravan.NewTopic[any](config.Timed(5), config.Counted(10))
}

func TestRetentionPolicyOption(t *testing.T) {
	as := assert.New(t)
	defer func() {
		as.Equal(errRetentionExplosion, recover())
	}()

	caravan.NewTopic[any](
		config.RetentionPolicy(explodingRetentionPolicy(false)),
	)
}

func (explodingRetentionPolicy) InitialState() retention.State {
	panic(errRetentionExplosion)
}

func (explodingRetentionPolicy) Retain(_ retention.State, _ *retention.Statistics) (retention.State, bool) {
	panic(errRetentionExplosion)
}
