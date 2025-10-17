package caravan

import (
	"github.com/kode4food/caravan/table"

	internal "github.com/kode4food/caravan/internal/table"
)

// NewTable instantiates a new Table given a set of column names
func NewTable[Key comparable, Value any](
	c ...table.ColumnName,
) (table.Table[Key, Value], error) {
	return internal.Make[Key, Value](c...)
}

// NewTableUpdater instantiates a new table Updater given a Table and a set of
// Key and Column Selectors
func NewTableUpdater[Msg any, Key comparable, Value any](
	t table.Table[Key, Value], k table.KeySelector[Msg, Key],
	c ...table.Column[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	return internal.MakeUpdater(t, k, c...)
}
