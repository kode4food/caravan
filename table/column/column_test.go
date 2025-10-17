package column_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan/table"
	"github.com/kode4food/caravan/table/column"
)

func TestColumn(t *testing.T) {
	as := assert.New(t)

	c := column.Make[any, string]("some-col", func(_ any) string {
		return "hello"
	})

	as.Equal(table.ColumnName("some-col"), c.Name())
	res := c.Select("anything")
	as.Equal("hello", res)
}
