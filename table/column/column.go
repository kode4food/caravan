package column

import "github.com/kode4food/caravan/table"

type column[Msg, Value any] struct {
	name  table.ColumnName
	value table.ValueSelector[Msg, Value]
}

// Make instantiates a new Column instance
func Make[Msg, Value any](
	n table.ColumnName, v table.ValueSelector[Msg, Value],
) table.Column[Msg, Value] {
	return &column[Msg, Value]{
		name:  n,
		value: v,
	}
}

func (c *column[_, _]) Name() table.ColumnName {
	return c.name
}

func (c *column[Msg, Value]) Select(m Msg) Value {
	return c.value(m)
}
