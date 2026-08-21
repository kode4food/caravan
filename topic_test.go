package caravan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
)

func TestNewTopic(t *testing.T) {
	assert.NotNil(t, caravan.NewTopic[string]())
}
