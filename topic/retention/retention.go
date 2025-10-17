package retention

import (
	"time"
)

type (
	// Policy describes and implements a policy that a Topic will use to
	// discard messages
	Policy interface {
		InitialState() State
		Retain(State, *Statistics) (State, bool)
	}

	// State allows a Policy to accumulate state between Retain calls
	State any

	// Statistics provides just enough information about the Log and a
	// range entries to be useful to a Policy
	Statistics struct {
		CurrentTime time.Time
		Log         *LogStatistics
		Entries     *EntriesStatistics
	}

	// LogStatistics provides Retention information about the Log
	LogStatistics struct {
		Length        uint64
		CursorOffsets []uint64
	}

	// EntriesStatistics provides Retention information about a range of
	// Log entries
	EntriesStatistics struct {
		FirstOffset    uint64
		LastOffset     uint64
		FirstTimestamp time.Time
		LastTimestamp  time.Time
	}
)
