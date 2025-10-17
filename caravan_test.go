package caravan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/stream/node"
	"github.com/kode4food/caravan/table"
	"github.com/kode4food/caravan/table/column"
)

func TestNewStream(t *testing.T) {
	as := assert.New(t)

	// Create a simple source that generates a few messages
	source := stream.Processor[stream.Source, int](
		func(c *context.Context[stream.Source, int]) {
			for i := 0; i < 3; i++ {
				c.Out <- i
			}
		},
	)

	// Create a processor that doubles values
	doubler := stream.Processor[int, int](
		func(c *context.Context[int, int]) {
			for msg := range c.In {
				c.Out <- msg * 2
			}
		},
	)

	// Create stream with the source and processor
	s := caravan.NewStream(source, doubler)
	as.NotNil(s)

	// Verify the stream can start and stop
	running := s.Start()
	as.True(running.IsRunning())

	err := running.Stop()
	as.NoError(err)
	as.False(running.IsRunning())
}

func TestNewStreamWithMultipleProcessors(t *testing.T) {
	as := assert.New(t)

	// Create a source that generates a single message
	source := stream.Processor[stream.Source, string](
		func(c *context.Context[stream.Source, string]) {
			c.Out <- "test"
		},
	)

	// Create multiple processors
	upper := stream.Processor[string, string](
		func(c *context.Context[string, string]) {
			for msg := range c.In {
				c.Out <- msg + "_upper"
			}
		},
	)

	lower := stream.Processor[string, string](
		func(c *context.Context[string, string]) {
			for msg := range c.In {
				c.Out <- msg + "_lower"
			}
		},
	)

	// Create stream with multiple processors
	s := caravan.NewStream(source, upper, lower)
	as.NotNil(s)

	// Start with custom handler to verify processing
	running := s.StartWith(func(a context.Advice, next func()) {
		// Just call next to continue default processing
		next()
	})

	err := running.Stop()
	as.NoError(err)
}

func TestNewTopic(t *testing.T) {
	as := assert.New(t)

	// Create a simple topic
	topic := caravan.NewTopic[string]()
	as.NotNil(topic)

	// Test that the topic was created successfully
	// Note: The actual Topic interface methods are defined in internal/topic
	// and may not be directly accessible from here
}

func TestNewTable(t *testing.T) {
	as := assert.New(t)

	// Create a table with columns
	tbl, err := caravan.NewTable[string, int]("col1", "col2", "col3")
	as.NoError(err)
	as.NotNil(tbl)

	// Test that the table was created successfully
	// Note: The actual Table interface methods are defined in internal/table
}

func TestNewTableUpdater(t *testing.T) {
	as := assert.New(t)

	// Define a message type
	type Message struct {
		ID    string
		Value int
	}

	// Create a table
	tbl, err := caravan.NewTable[string, int]("value")
	as.NoError(err)

	// Create key selector
	keySelector := table.KeySelector[Message, string](func(msg Message) string {
		return msg.ID
	})

	// Create column selector
	valueColumn := column.Make[Message, int](
		"value",
		table.ValueSelector[Message, int](func(msg Message) int {
			return msg.Value
		}),
	)

	// Create updater
	updater, err := caravan.NewTableUpdater(tbl, keySelector, valueColumn)
	as.NoError(err)
	as.NotNil(updater)

	// Test that the updater was created successfully
}

func TestNewStreamIntegration(t *testing.T) {
	as := assert.New(t)

	counter := 0
	// Create a generator source
	source := node.Generate[int](func() (int, bool) {
		if counter < 5 {
			counter++
			return counter, true
		}
		return 0, false
	})

	// Create a filter processor
	filter := node.Filter(func(v int) bool {
		return v%2 == 0
	})

	// Create the stream
	s := caravan.NewStream(source, filter)
	as.NotNil(s)

	running := s.Start()
	as.True(running.IsRunning())

	err := running.Stop()
	as.NoError(err)
}
