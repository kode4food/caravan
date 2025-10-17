package retention

type (
	// ConsumedPolicy describes a Policy that discards messages that have
	// been consumed by all active Consumers
	ConsumedPolicy interface {
		Policy
	}

	consumedPolicy struct{}
)

var _consumedPolicy = &consumedPolicy{}

// MakeConsumedPolicy returns a Policy that allows for the discarding of messages
// that have already been consumed by active Consumers
func MakeConsumedPolicy() ConsumedPolicy {
	return _consumedPolicy
}

func (*consumedPolicy) InitialState() State {
	return nil
}

func (*consumedPolicy) Retain(s State, r *Statistics) (State, bool) {
	off := r.Log.CursorOffsets
	if len(off) > 0 {
		for _, o := range off {
			if o <= r.Entries.LastOffset {
				return nil, true
			}
		}
	}
	return s, false
}
