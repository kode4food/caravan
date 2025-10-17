package config

import (
	"github.com/kode4food/caravan/topic/backoff"
	"github.com/kode4food/caravan/topic/retention"
)

// ApplyDefaults copies a Config instance and applies defaults to it
func ApplyDefaults(c *Config) *Config {
	res := *c
	if res.RetentionPolicy == nil {
		res.RetentionPolicy = retention.MakePermanentPolicy()
	}
	if res.BackoffGenerator == nil {
		res.BackoffGenerator = backoff.DefaultGenerator
	}
	if res.SegmentIncrement == 0 {
		res.SegmentIncrement = DefaultSegmentIncrement
	}
	return &res
}

// ApplyOptions applies Options to a topic
func ApplyOptions(c *Config, options ...Option) error {
	for _, o := range options {
		if err := o(c); err != nil {
			return err
		}
	}
	return nil
}

// Defaults applies default settings to a topic if they weren't explicitly set
// through some other Option
func Defaults(c *Config) error {
	*c = *ApplyDefaults(c)
	return nil
}
