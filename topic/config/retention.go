package config

import (
	"errors"
	"time"

	"github.com/kode4food/caravan/topic/retention"
)

// Error messages
const (
	ErrRetentionPolicyAlreadySet = "retention policy already set in topic"
)

// Consumed applies a consumed Policy to the Topic
func Consumed(c *Config) error {
	policy := retention.MakeConsumedPolicy()
	return maybeSetRetentionPolicy(c, policy)
}

// Counted applies a counted Policy to the Topic
func Counted(c retention.Count) Option {
	return func(t *Config) error {
		policy := retention.MakeCountedPolicy(c)
		return maybeSetRetentionPolicy(t, policy)
	}
}

// Permanent applies a permanent Policy to the Topic
func Permanent(c *Config) error {
	policy := retention.MakePermanentPolicy()
	return maybeSetRetentionPolicy(c, policy)
}

// Timed applies a timed Policy to the Topic
func Timed(d time.Duration) Option {
	return func(t *Config) error {
		policy := retention.MakeTimedPolicy(d)
		return maybeSetRetentionPolicy(t, policy)
	}
}

// RetentionPolicy applies a provided retention Policy to the Topic
func RetentionPolicy(p retention.Policy) Option {
	return func(t *Config) error {
		return maybeSetRetentionPolicy(t, p)
	}
}

func maybeSetRetentionPolicy(c *Config, p retention.Policy) error {
	if c.RetentionPolicy == nil {
		c.RetentionPolicy = p
		return nil
	}
	return errors.New(ErrRetentionPolicyAlreadySet)
}
