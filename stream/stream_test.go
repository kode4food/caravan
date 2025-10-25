package stream_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	testutil "github.com/kode4food/caravan/internal/testing"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
)

func TestProcessorStart(t *testing.T) {
	as := assert.New(t)

	h := testutil.NewTestSlogHandler()
	oldHandler := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(oldHandler)

	p := stream.Processor[string, string](
		func(c *context.Context[string, string]) {
			// intentionally trip the late return debug
			time.Sleep(10 * time.Millisecond)
		},
	)

	monitor := make(chan context.Advice)
	c := context.Make[string, string](
		make(chan context.Done), monitor, nil, nil,
	)

	p.Start(c)
	time.Sleep(50 * time.Millisecond)

	select {
	case r := <-h.Logs:
		as.Contains(r.Message, stream.ErrReturnedLate.Error())
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for debug log")
	}
}
