package retention

type (
	// UnaryPolicy is a Policy that applies some additional logic to a
	// supplied Policy
	UnaryPolicy interface {
		Policy
		Policy() Policy
	}

	// NotPolicy is a Policy that negates the retention logic of its
	// supplied Policy
	NotPolicy interface {
		UnaryPolicy
	}

	// BinaryPolicy is a Policy where two Policies are combined in order
	// to determine retention policy
	BinaryPolicy interface {
		Policy
		Left() Policy
		Right() Policy
	}

	// AndPolicy is a Policy from which both Policies must request that
	// messages be retained in order to do so
	AndPolicy interface {
		BinaryPolicy
	}

	// OrPolicy is a Policy from which either Policy can request that
	// messages be retained in order to do so
	OrPolicy interface {
		BinaryPolicy
	}

	unaryPolicy struct {
		policy Policy
	}

	notPolicy struct {
		unaryPolicy
	}

	binaryPolicy struct {
		left  Policy
		right Policy
	}

	andPolicy struct {
		binaryPolicy
	}

	orPolicy struct {
		binaryPolicy
	}

	binaryPolicyState struct {
		left  State
		right State
	}
)

// Not returns a Policy that negates the logic of its supplied Policy
func Not(policy Policy) NotPolicy {
	return &notPolicy{
		unaryPolicy: unaryPolicy{
			policy: policy,
		},
	}
}

func (p *notPolicy) Retain(state State, r *Statistics) (State, bool) {
	state, ok := p.policy.Retain(state, r)
	return state, !ok
}

func (p *unaryPolicy) InitialState() State {
	return p.policy.InitialState()
}

func (p *unaryPolicy) Policy() Policy {
	return p.policy
}

// And returns a Policy from which both Policies must request that messages be
// retained in order to do so
func And(left Policy, right Policy) AndPolicy {
	return &andPolicy{
		binaryPolicy: binaryPolicy{
			left:  left,
			right: right,
		},
	}
}

func (p *andPolicy) Retain(s State, r *Statistics) (State, bool) {
	state, lok, rok := p.retain(s, r)
	return state, lok && rok
}

// Or returns a Policy from which either Policy can request that messages be
// retained in order to do so
func Or(left Policy, right Policy) OrPolicy {
	return &orPolicy{
		binaryPolicy: binaryPolicy{
			left:  left,
			right: right,
		},
	}
}

func (p *orPolicy) Retain(s State, r *Statistics) (State, bool) {
	state, lok, rok := p.retain(s, r)
	return state, lok || rok
}

func (p *binaryPolicy) InitialState() State {
	return &binaryPolicyState{
		left:  p.left.InitialState(),
		right: p.right.InitialState(),
	}
}

func (p *binaryPolicy) Left() Policy {
	return p.left
}

func (p *binaryPolicy) Right() Policy {
	return p.right
}

func (p *binaryPolicy) retain(s State, r *Statistics) (State, bool, bool) {
	var lok, rok bool
	state := s.(*binaryPolicyState)
	state.left, lok = p.left.Retain(state.left, r)
	state.right, rok = p.right.Retain(state.right, r)
	return state, lok, rok
}
