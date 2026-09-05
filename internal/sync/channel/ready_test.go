package channel_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/internal/sync/channel"
)

func TestReadyWait(t *testing.T) {
	as := assert.New(t)

	w := channel.NewReadyWait()
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

func TestReadyWaitConcurrentNotifyClose(t *testing.T) {
	w := channel.NewReadyWait()
	start := make(chan struct{})
	closed := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		<-start
		for {
			select {
			case <-closed:
				w.Notify()
				return
			default:
				w.Notify()
			}
		}
	})

	wg.Go(func() {
		<-start
		w.Close()
		close(closed)
	})

	close(start)
	wg.Wait()
}
