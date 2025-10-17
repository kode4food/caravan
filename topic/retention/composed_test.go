package retention_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/topic/retention"
)

type boolPolicy bool

func TestNotPolicy(t *testing.T) {
	as := assert.New(t)
	tp := boolPolicy(true)
	fp := boolPolicy(false)

	p := retention.Not(tp)
	s := p.InitialState()
	as.Nil(s)
	as.Equal(tp, p.Policy())

	_, ok := retention.Not(tp).Retain(nil, nil)
	as.False(ok)

	_, ok = retention.Not(fp).Retain(nil, nil)
	as.True(ok)
}

func TestAndPolicy(t *testing.T) {
	as := assert.New(t)
	l := retention.MakeConsumedPolicy()
	r := retention.MakePermanentPolicy()
	p := retention.And(l, r)
	as.NotNil(p)
	as.NotNil(p.InitialState())
	as.Equal(l, p.Left())
	as.Equal(r, p.Right())
}

func TestAndCompose(t *testing.T) {
	as := assert.New(t)
	tp := boolPolicy(true)
	fp := boolPolicy(false)
	s := retention.And(tp, fp).InitialState()

	_, ok := retention.And(tp, fp).Retain(s, nil)
	as.False(ok)

	_, ok = retention.And(tp, tp).Retain(s, nil)
	as.True(ok)

	_, ok = retention.And(fp, fp).Retain(s, nil)
	as.False(ok)
}

func TestOrPolicy(t *testing.T) {
	as := assert.New(t)
	l := retention.MakeConsumedPolicy()
	r := retention.MakePermanentPolicy()
	p := retention.Or(l, r)
	as.NotNil(p)
	as.NotNil(p.InitialState())
	as.Equal(l, p.Left())
	as.Equal(r, p.Right())
}

func TestOrCompose(t *testing.T) {
	as := assert.New(t)
	tp := boolPolicy(true)
	fp := boolPolicy(false)
	s := retention.Or(tp, fp).InitialState()

	_, ok := retention.Or(tp, fp).Retain(s, nil)
	as.True(ok)

	_, ok = retention.Or(tp, tp).Retain(s, nil)
	as.True(ok)

	_, ok = retention.Or(fp, fp).Retain(s, nil)
	as.False(ok)
}

func (boolPolicy) InitialState() retention.State {
	return nil
}

func (b boolPolicy) Retain(s retention.State, _ *retention.Statistics) (retention.State, bool) {
	return s, bool(b)
}
