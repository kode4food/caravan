package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/topic/backoff"
	"github.com/kode4food/caravan/topic/config"
)

const errBackoffExplosion = "intentional backoff explosion"

func TestBackoffConflict(t *testing.T) {
	as := assert.New(t)

	defer func() {
		rec := recover()
		as.NotNil(rec)
		as.Errorf(rec.(error), config.ErrBackoffAlreadySet)
	}()

	caravan.NewTopic[any](
		config.FixedBackoffSequence(10),
		config.FibonacciBackoffSequence(time.Microsecond, 100),
	)
}

func TestBackoffGeneratorOption(t *testing.T) {
	as := assert.New(t)
	defer func() {
		as.Equal(errBackoffExplosion, recover())
	}()

	caravan.NewTopic[any](
		config.BackoffGenerator(func() backoff.Next {
			panic(errBackoffExplosion)
		}),
	).NewConsumer()
}
