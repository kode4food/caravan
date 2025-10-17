package channel_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/internal/sync/channel"
)

func TestReadyWait(t *testing.T) {
	as := assert.New(t)

	w := channel.MakeReadyWait()
	go func() {
		as.NotNil(<-w.Wait())
	}()

	w.Notify()
	w.Close()

	defer func() {
		as.Error(recover().(error))
	}()
	w.Close()
}
