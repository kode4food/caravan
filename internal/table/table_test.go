package table_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/table"
)

func TestTable(t *testing.T) {
	as := assert.New(t)

	tbl, err := caravan.NewTable[string, any]("name", "age")
	as.NotNil(tbl)
	as.Nil(err)

	getter, err := tbl.Getter("name", "age")
	as.NotNil(getter)
	as.Nil(err)

	setter, err := tbl.Setter("name", "age")
	as.NotNil(setter)
	as.Nil(err)

	firstID := "first id"
	secondID := "second id"

	err = setter(firstID, "bill", 42)
	as.Nil(err)

	err = setter(secondID, "carol", 47)
	as.Nil(err)

	res, _ := getter(firstID)
	as.Equal([]any{"bill", 42}, res)

	res, _ = getter(secondID)
	as.Equal([]any{"carol", 47}, res)

	missing := "missing"
	res, err = getter(missing)
	as.Nil(res)
	as.EqualError(err, fmt.Sprintf(table.ErrKeyNotFound, missing))
}

func TestBadTable(t *testing.T) {
	as := assert.New(t)
	tbl, err := caravan.NewTable[string, any](
		"column-1", "column-2", "column-1",
	)
	as.Nil(tbl)
	as.Errorf(err, table.ErrDuplicateColumnName, "column-1")
}

func TestMissingColumn(t *testing.T) {
	as := assert.New(t)

	tbl, err := caravan.NewTable[string, any]("column-1", "column-2")
	as.NotNil(tbl)
	as.Nil(err)

	as.Equal([]table.ColumnName{"column-1", "column-2"}, tbl.Columns())
	sel, err := tbl.Getter("not-found")
	as.Nil(sel)
	as.EqualError(err, fmt.Sprintf(table.ErrColumnNotFound, "not-found"))
}

func TestCompetingSetters(t *testing.T) {
	as := assert.New(t)

	tbl, _ := caravan.NewTable[string, any]("name", "age")
	allSetter, _ := tbl.Setter("name", "age")
	getter, _ := tbl.Getter("name", "age")

	nameSetter, err := tbl.Setter("name")
	as.NotNil(nameSetter)
	as.Nil(err)

	ageSetter, err := tbl.Setter("age")
	as.NotNil(ageSetter)
	as.Nil(err)

	as.Nil(allSetter("1", "bob", 42))
	as.Nil(allSetter("2", "june", 36))

	row, _ := getter("1")
	as.Equal([]any{"bob", 42}, row)
	row, _ = getter("2")
	as.Equal([]any{"june", 36}, row)

	as.Nil(nameSetter("1", "robert"))
	as.Nil(ageSetter("2", 41))

	row, _ = getter("1")
	as.Equal([]any{"robert", 42}, row)

	row, _ = getter("2")
	as.Equal([]any{"june", 41}, row)
}

func TestBadSetter(t *testing.T) {
	as := assert.New(t)

	tbl, err := caravan.NewTable[string, any]("column-1", "column-2")
	as.NotNil(tbl)
	as.Nil(err)

	s, err := tbl.Setter("column-1", "column-2", "column-1")
	as.Nil(s)
	as.Errorf(err, table.ErrDuplicateColumnName, "column-1")

	s, err = tbl.Setter("column-1", "column-2", "column-3")
	as.Nil(s)
	as.EqualError(err, fmt.Sprintf(table.ErrColumnNotFound, "column-3"))

	s, err = tbl.Setter("column-1", "column-2")
	as.NotNil(s)
	as.Nil(err)

	err = s("some-key", "too few")
	as.Errorf(err, fmt.Sprintf(table.ErrValueCountRequired, 2, 1))

	err = s("some-key", "one", "too", "many")
	as.Errorf(err, fmt.Sprintf(table.ErrValueCountRequired, 2, 3))
}

func TestTableDelete(t *testing.T) {
	as := assert.New(t)

	tbl, err := caravan.NewTable[string, string]("name", "city")
	as.NotNil(tbl)
	as.Nil(err)

	setter, _ := tbl.Setter("name", "city")
	getter, _ := tbl.Getter("name", "city")

	// Add some rows
	as.Nil(setter("user-1", "alice", "nyc"))
	as.Nil(setter("user-2", "bob", "sf"))
	as.Nil(setter("user-3", "carol", "la"))

	as.Equal(3, tbl.Count())

	// Delete existing key
	err = tbl.Delete("user-2")
	as.Nil(err)
	as.Equal(2, tbl.Count())

	// Verify row is gone
	_, err = getter("user-2")
	as.EqualError(err, fmt.Sprintf(table.ErrKeyNotFound, "user-2"))

	// Other rows still exist
	row, err := getter("user-1")
	as.Nil(err)
	as.Equal([]string{"alice", "nyc"}, row)

	// Try to delete non-existent key
	err = tbl.Delete("user-999")
	as.EqualError(err, fmt.Sprintf(table.ErrKeyNotFoundDelete, "user-999"))
}

func TestTableKeys(t *testing.T) {
	as := assert.New(t)

	tbl, _ := caravan.NewTable[string, int]("value")
	setter, _ := tbl.Setter("value")

	// Empty table
	keys := tbl.Keys()
	as.Equal(0, len(keys))

	// Add rows
	as.Nil(setter("key-1", 100))
	as.Nil(setter("key-2", 200))
	as.Nil(setter("key-3", 300))

	keys = tbl.Keys()
	as.Equal(3, len(keys))
	as.Contains(keys, "key-1")
	as.Contains(keys, "key-2")
	as.Contains(keys, "key-3")
}

func TestTableCount(t *testing.T) {
	as := assert.New(t)

	tbl, _ := caravan.NewTable[int, string]("name")
	setter, _ := tbl.Setter("name")

	// Empty table
	as.Equal(0, tbl.Count())

	// Add rows
	as.Nil(setter(1, "one"))
	as.Equal(1, tbl.Count())

	as.Nil(setter(2, "two"))
	as.Equal(2, tbl.Count())

	as.Nil(setter(3, "three"))
	as.Equal(3, tbl.Count())

	// Update existing row shouldn't change count
	as.Nil(setter(2, "TWO"))
	as.Equal(3, tbl.Count())

	// Delete row
	tbl.Delete(2)
	as.Equal(2, tbl.Count())
}

func TestTableRange(t *testing.T) {
	as := assert.New(t)

	tbl, _ := caravan.NewTable[string, any]("name", "age")
	setter, _ := tbl.Setter("name", "age")

	// Add rows
	as.Nil(setter("user-1", "alice", 25))
	as.Nil(setter("user-2", "bob", 30))
	as.Nil(setter("user-3", "carol", 35))

	// Collect all rows
	visited := map[string][]any{}
	tbl.Range(func(k string, v []any) bool {
		visited[k] = v
		return true
	})

	as.Equal(3, len(visited))
	as.Equal([]any{"alice", 25}, visited["user-1"])
	as.Equal([]any{"bob", 30}, visited["user-2"])
	as.Equal([]any{"carol", 35}, visited["user-3"])
}

func TestTableRangeEarlyExit(t *testing.T) {
	as := assert.New(t)

	tbl, _ := caravan.NewTable[int, string]("value")
	setter, _ := tbl.Setter("value")

	// Add rows
	for i := 1; i <= 10; i++ {
		as.Nil(setter(i, fmt.Sprintf("value-%d", i)))
	}

	// Visit only first 3 rows
	count := 0
	tbl.Range(func(k int, v []string) bool {
		count++
		return count < 3
	})

	as.Equal(3, count)
}
