package retention

import "time"

type (
	// TimedPolicy describes a Policy that only retains messages retains
	// messages produced in the last specified Duration
	TimedPolicy interface {
		Policy
		Duration() time.Duration
	}

	timedPolicy struct {
		duration time.Duration
	}
)

// MakeTimedPolicy returns a Policy that only retains messages produced in the
// last specified Duration
func MakeTimedPolicy(d time.Duration) TimedPolicy {
	return &timedPolicy{
		duration: d,
	}
}

func (p *timedPolicy) Duration() time.Duration {
	return p.duration
}

func (*timedPolicy) InitialState() State {
	return nil
}

func (p *timedPolicy) Retain(s State, r *Statistics) (State, bool) {
	diff := r.CurrentTime.Sub(r.Entries.LastTimestamp)
	return s, diff <= p.duration
}
