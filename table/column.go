package table

type (
	// Column describes a column, including its name and a ValueSelector for
	// retrieving the column's value from a message
	Column[Msg, Value any] interface {
		Name() ColumnName
		Select(Msg) Value
	}

	column[Msg, Value any] struct {
		name  ColumnName
		value ValueSelector[Msg, Value]
	}
)

// MakeColumn instantiates a new Column instance
func MakeColumn[Msg, Value any](
	n ColumnName, v ValueSelector[Msg, Value],
) Column[Msg, Value] {
	return &column[Msg, Value]{
		name:  n,
		value: v,
	}
}

func (c *column[_, _]) Name() ColumnName {
	return c.name
}

func (c *column[Msg, Value]) Select(m Msg) Value {
	return c.value(m)
}
