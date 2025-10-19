package table_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/table"
)

type tableRow struct {
	key  string
	name string
	age  int
}

func TestUpdater(t *testing.T) {
	as := assert.New(t)

	tbl, err := caravan.NewTable[string, any]("name", "age")
	as.NotNil(tbl)
	as.Nil(err)

	getter, err := tbl.Getter("name", "age")
	as.NotNil(getter)
	as.Nil(err)

	updater, err := caravan.NewTableUpdater(tbl,
		func(e *tableRow) string {
			return e.key
		},
		table.MakeColumn("name", func(r *tableRow) any {
			return r.name
		}),
		table.MakeColumn("age", func(r *tableRow) any {
			return r.age
		}),
	)
	as.NotNil(updater)
	as.Nil(err)

	firstID := "first id"
	secondID := "second id"

	err = updater.Update(&tableRow{
		key:  firstID,
		name: "bill",
		age:  42,
	})
	as.Nil(err)
	res, _ := getter(firstID)
	as.Equal([]any{"bill", 42}, res)

	err = updater.Update(&tableRow{
		key:  secondID,
		name: "carol",
		age:  47,
	})
	as.Nil(err)
	res, _ = getter(secondID)
	as.Equal([]any{"carol", 47}, res)

	sel, err := tbl.Getter("age", "name")
	as.NotNil(sel)
	as.Nil(err)

	res, err = sel(secondID)
	as.Nil(err)
	as.Equal([]any{47, "carol"}, res)

	res, err = sel(firstID)
	as.Nil(err)
	as.Equal([]any{42, "bill"}, res)

	missing := "missing"
	res, err = sel(missing)
	as.Nil(res)
	as.EqualError(err, fmt.Sprintf(table.ErrKeyNotFound, missing))
}
