package retention

type (
	// CountedPolicy describes a Policy that only retains the most recent
	// specified Count of messages
	CountedPolicy interface {
		Policy
		Count() Count
	}

	// Count is the retained size of a Topic stream
	Count uint64

	countedPolicy struct {
		count Count
	}
)

// MakeCountedPolicy returns a Policy that only retains the most recent
// specified count of messages
func MakeCountedPolicy(c Count) CountedPolicy {
	return &countedPolicy{
		count: c,
	}
}

func (p *countedPolicy) Count() Count {
	return p.count
}

func (*countedPolicy) InitialState() State {
	return nil
}

func (p *countedPolicy) Retain(s State, r *Statistics) (State, bool) {
	return s, Count(r.Entries.LastOffset) >= Count(r.Log.Length)-p.count
}
