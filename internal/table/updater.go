package table

import "github.com/kode4food/caravan/table"

type updater[Msg any, Key comparable, Value any] struct {
	table   table.Table[Key, Value]
	key     table.KeySelector[Msg, Key]
	setter  table.Setter[Key, Value]
	columns []table.Column[Msg, Value]
}

// NewUpdater returns an Updater for the provided selectors
func NewUpdater[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value], key table.KeySelector[Msg, Key],
	cols ...table.Column[Msg, Value],
) (table.Updater[Msg, Key, Value], error) {
	names := make([]table.ColumnName, len(cols))
	for i, c := range cols {
		names[i] = c.Name()
	}
	setter, err := tbl.Setter(names...)
	if err != nil {
		return nil, err
	}
	return &updater[Msg, Key, Value]{
		key:     key,
		columns: cols,
		table:   tbl,
		setter:  setter,
	}, nil
}

// Key returns the Updater's key selector
func (u *updater[Msg, Key, _]) Key() table.KeySelector[Msg, Key] {
	return u.key
}

// Columns returns the Columns written by the Updater
func (u *updater[Msg, _, Value]) Columns() []table.Column[Msg, Value] {
	return u.columns
}

// Update adds or overwrites a message in the Table. The message is associated
// with a Key that is selected from the message using the Table's KeySelector
func (u *updater[Msg, _, Value]) Update(msg Msg) error {
	k := u.key(msg)
	row := make([]Value, len(u.columns))
	for i, s := range u.columns {
		row[i] = s.Select(msg)
	}
	return u.setter(k, row...)
}
