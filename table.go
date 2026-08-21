package caravan

import (
	_table "github.com/kode4food/caravan/internal/table"
	"github.com/kode4food/caravan/table"
)

// NewTable returns an empty Table with the provided columns
func NewTable[Key comparable, Value any](
	cols ...table.ColumnName,
) (table.Table[Key, Value], error) {
	return _table.New[Key, Value](cols...)
}

// NewTableUpdater returns an Updater for the provided key and columns
func NewTableUpdater[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value], key table.KeySelector[Msg, Key],
	cols ...table.Column[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	return _table.NewUpdater(tbl, key, cols...)
}
