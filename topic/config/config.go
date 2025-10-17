package config

import (
	"github.com/kode4food/caravan/topic/backoff"
	"github.com/kode4food/caravan/topic/retention"
)

type (
	// Config conveys the properties of a Topic that one can configure using
	// Options
	Config struct {
		RetentionPolicy  retention.Policy
		BackoffGenerator backoff.Generator
		SegmentIncrement uint16
	}

	// Option applies an option to a topic configuration instance
	Option func(*Config) error
)

// Defaults
const (
	DefaultSegmentIncrement = 32
)
