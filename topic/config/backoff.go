package config

import (
	"errors"
	"time"

	"github.com/kode4food/caravan/topic/backoff"
)

// Error messages
const (
	ErrBackoffAlreadySet = "backoff algorithm already set in topic"
)

// FixedBackoffSequence configures a Topic with a Generator wherein every
// Duration returned is the specified fixed amount
func FixedBackoffSequence(ms time.Duration) Option {
	return func(c *Config) error {
		gen := backoff.MakeFixedGenerator(ms)
		return maybeSetBackoffGenerator(c, gen)
	}
}

// FibonacciBackoffSequence creates a Generator wherein every Duration follows
// the fibonacci sequence up to the specified maximum duration. Each fibonacci
// number is returned as many times as its value
func FibonacciBackoffSequence(unit, max time.Duration) Option {
	return func(c *Config) error {
		gen := backoff.MakeFibonacciGenerator(unit, max)
		return maybeSetBackoffGenerator(c, gen)
	}
}

// BackoffGenerator applies a provided backoff Generator to the Topic
func BackoffGenerator(b backoff.Generator) Option {
	return func(t *Config) error {
		return maybeSetBackoffGenerator(t, b)
	}
}

func maybeSetBackoffGenerator(c *Config, b backoff.Generator) error {
	if c.BackoffGenerator == nil {
		c.BackoffGenerator = b
		return nil
	}
	return errors.New(ErrBackoffAlreadySet)
}
