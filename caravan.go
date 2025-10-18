package caravan

import (
	streamImpl "github.com/kode4food/caravan/internal/stream"
	tableImpl "github.com/kode4food/caravan/internal/table"
	topicImpl "github.com/kode4food/caravan/internal/topic"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/node"
	"github.com/kode4food/caravan/table"
	"github.com/kode4food/caravan/topic"
	"github.com/kode4food/caravan/topic/config"
)

// NewTopic instantiates a new Topic
func NewTopic[Msg any](o ...config.Option) topic.Topic[Msg] {
	return topicImpl.Make[Msg](o...)
}

// NewStream instantiates a new stream, given a set of Processors
func NewStream[Msg any](
	source stream.Processor[stream.Source, Msg],
	rest ...stream.Processor[Msg, Msg],
) stream.Stream {
	return streamImpl.Make(source, node.Subprocess(rest...))
}

// NewTable instantiates a new Table given a set of column names
func NewTable[Key comparable, Value any](
	c ...table.ColumnName,
) (table.Table[Key, Value], error) {
	return tableImpl.Make[Key, Value](c...)
}

// NewTableUpdater instantiates a new table Updater given a Table and a set of
// Key and Column Selectors
func NewTableUpdater[Msg any, Key comparable, Value any](
	t table.Table[Key, Value], k table.KeySelector[Msg, Key],
	c ...table.Column[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	return tableImpl.MakeUpdater(t, k, c...)
}
