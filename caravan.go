package caravan

import (
	streamImpl "github.com/kode4food/caravan/internal/stream"
	tableImpl "github.com/kode4food/caravan/internal/table"
	topicImpl "github.com/kode4food/caravan/internal/topic"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/node"
	"github.com/kode4food/caravan/table"
	"github.com/kode4food/caravan/topic"
)

// NewTopic instantiates a new Topic
func NewTopic[Msg any]() topic.Topic[Msg] {
	return topicImpl.Make[Msg]()
}

// NewStream instantiates a new stream, given a set of Processors
func NewStream[Msg any](
	source stream.Processor[stream.Source, Msg],
	rest ...stream.Processor[Msg, Msg],
) stream.Stream {
	return BasicStream(source, node.Subprocess(rest...))
}

// BasicStream instantiates a new Stream that must be started with the Start
// method.
func BasicStream[In, Out any](
	source stream.Processor[stream.Source, In], rest stream.Processor[In, Out],
) stream.Stream {
	return streamImpl.Make(source, rest)
}

// NewTable instantiates a new Table given a set of column names
func NewTable[Key comparable, Value any](
	cols ...table.ColumnName,
) (table.Table[Key, Value], error) {
	return tableImpl.Make[Key, Value](cols...)
}

// NewTableUpdater instantiates a new table Updater given a Table and a set of
// Key and Column Selectors
func NewTableUpdater[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value], key table.KeySelector[Msg, Key],
	cols ...table.Column[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	return tableImpl.MakeUpdater(tbl, key, cols...)
}
