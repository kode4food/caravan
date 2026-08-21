package table

// Column describes a named value selector
type Column[Msg, Value any] struct {
	value ValueSelector[Msg, Value]
	name  ColumnName
}

// NewColumn returns a named value selector
func NewColumn[Msg, Value any](
	n ColumnName, v ValueSelector[Msg, Value],
) Column[Msg, Value] {
	return Column[Msg, Value]{
		name:  n,
		value: v,
	}
}

// Name returns the Column name
func (c Column[_, _]) Name() ColumnName {
	return c.name
}

// Select returns the Column value selected from a message
func (c Column[Msg, Value]) Select(m Msg) Value {
	return c.value(m)
}
