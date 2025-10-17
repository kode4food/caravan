package backoff_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/topic/backoff"
)

func TestFixed(t *testing.T) {
	as := assert.New(t)
	next := backoff.MakeFixedGenerator(10 * time.Microsecond)()
	as.Equal(time.Microsecond*10, next())
	as.Equal(time.Microsecond*10, next())
	as.Equal(time.Microsecond*10, next())
	as.Equal(time.Microsecond*10, next())
}

func TestDefault(t *testing.T) {
	as := assert.New(t)
	next := backoff.DefaultGenerator()
	as.Equal(time.Microsecond*1, next())
	as.Equal(time.Microsecond*2, next())
	as.Equal(time.Microsecond*3, next())
	as.Equal(time.Microsecond*5, next())
	as.Equal(time.Microsecond*8, next())
	as.Equal(time.Microsecond*13, next())
}

func TestFibonacci(t *testing.T) {
	as := assert.New(t)
	next := backoff.MakeFibonacciGenerator(time.Millisecond, time.Millisecond*5)()
	as.Equal(time.Millisecond*1, next())
	as.Equal(time.Millisecond*2, next())
	as.Equal(time.Millisecond*3, next())
	as.Equal(time.Millisecond*5, next())

	// Should keep providing 5 indefinitely
	as.Equal(time.Millisecond*5, next())
	as.Equal(time.Millisecond*5, next())
}
