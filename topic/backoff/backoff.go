package backoff

import "time"

type (
	// Generator creates a channel of Durations that can be used to drive
	// timeout logic for Consumer channels that are not in a receiving
	// state, the func returned resets the sequence
	Generator func() Next

	// Next is a function that returns the next timeout duration and
	// internally progresses the backoff sequence
	Next func() time.Duration
)

const (
	defaultUnit = time.Microsecond
	defaultMax  = 125 * time.Millisecond
)

// MakeFixedGenerator creates a fixed generator for retrying at intervals of
// the specified Duration
func MakeFixedGenerator(d time.Duration) Generator {
	return func() Next {
		return func() time.Duration {
			return d
		}
	}
}

// DefaultGenerator is fibonacci generator configured for retrying at intervals
// that backoff using the fibonacci sequence up to a maximum Duration of 125
// milliseconds
var DefaultGenerator = MakeFibonacciGenerator(defaultUnit, defaultMax)

// MakeFibonacciGenerator creates a fibonacci generator for retrying at
// intervals that backoff using the fibonacci sequence up to a maximum Duration
// specified
func MakeFibonacciGenerator(unit, max time.Duration) Generator {
	return func() Next {
		prev := time.Duration(0)
		curr := unit
		return func() time.Duration {
			if curr+prev > max {
				return max
			}
			tmp := curr
			curr = tmp + prev
			prev = tmp
			return curr
		}
	}
}
