package topic

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/kode4food/caravan/topic"
	"github.com/kode4food/caravan/topic/config"
)

type (
	// ErrorWrapper is a function that wraps an error. It is returned by
	// WrapStackTrace to attach stack information to a standard error
	ErrorWrapper func(error) error

	debugger struct {
		sync.Mutex
		enabled bool
		topic   topic.Topic[error]
	}
)

// Environment variables
const (
	CaravanDebug = "CARAVAN_DEBUG"
)

// Error messages
const (
	MsgInstantiationTrace = "stack at time of instantiation"
)

var (
	trueMatcher = regexp.MustCompile(`^\s*(TRUE|YES|OK|1)\s*$`)

	Debug = &debugger{}
)

func (d *debugger) getDebugTopic() topic.Topic[error] {
	d.Lock()
	defer d.Unlock()
	if d.topic == nil {
		d.topic = Make[error](config.Consumed)
	}
	return d.topic
}

// Enable all debugging
func (d *debugger) Enable() {
	d.Lock()
	defer d.Unlock()
	d.enabled = true
}

// IsEnabled returns whether debugging is enabled
func (d *debugger) IsEnabled() bool {
	d.Lock()
	defer d.Unlock()
	return d.enabled
}

// WithProducer performs a callback, providing to it a debugging Producer whose
// lifecycle is managed by WithProducer itself
func (d *debugger) WithProducer(with func(p topic.Producer[error])) {
	p := d.getDebugTopic().NewProducer()
	with(p)
	p.Close()
}

// WithConsumer performs a callback, providing to it a debugging Consumer whose
// lifecycle is managed by WithConsumer itself
func (d *debugger) WithConsumer(with func(c topic.Consumer[error])) {
	c := d.getDebugTopic().NewConsumer()
	with(c)
	c.Close()
}

// TailLogTo will send debug Topic errors to the specified io.Writer. When
// CARAVAN_DEBUG is set, all reported errors are automatically tailed to
// os.Stderr
func (d *debugger) TailLogTo(w io.Writer) {
	go func() {
		d.WithConsumer(func(c topic.Consumer[error]) {
			for err := range c.Receive() {
				_, _ = fmt.Fprintf(w, "%s\n", err)
			}
		})
	}()
}

// WrapStackTrace returns an ErrorWrapper that attaches Stack information to an
// error based on the call stack when this function is invoked
func WrapStackTrace(msg string) ErrorWrapper {
	stack := string(debug.Stack())
	return func(e error) error {
		return fmt.Errorf("%w\n%s:\n%s", e, msg, stack)
	}
}

func init() {
	if s, ok := os.LookupEnv(CaravanDebug); ok {
		if trueMatcher.MatchString(strings.ToUpper(s)) {
			Debug.Enable()
			Debug.TailLogTo(os.Stderr)
		}
	}
}
