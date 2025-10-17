package node_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/stream/node"
	"github.com/kode4food/caravan/table"
	"github.com/kode4food/caravan/table/column"
)

type row struct {
	id    string
	name  string
	value string
}

func makeTestTable() (
	table.Table[string, string],
	table.Updater[*row, string, string],
) {
	tbl, _ := caravan.NewTable[string, string]("id", "name", "value")

	updater, _ := caravan.NewTableUpdater(tbl,
		func(r *row) string {
			return r.id
		},
		column.Make("id", func(r *row) string {
			return r.id
		}),
		column.Make("name", func(r *row) string {
			return r.name
		}),
		column.Make("value", func(r *row) string {
			return r.value
		}),
	)

	return tbl, updater
}

func TestTableUpdater(t *testing.T) {
	as := assert.New(t)

	tbl, u := makeTestTable()

	updater := node.TableUpdater(u)
	lookup, _ := node.TableLookup(tbl, "value",
		func(r *row) string {
			return r.id
		},
	)

	tester := node.Bind(updater, lookup)
	done := make(chan context.Done)
	in := make(chan *row)
	out := make(chan string)

	tester.Start(context.Make(done, make(chan context.Advice), in, out))
	in <- &row{
		id:    "some id",
		name:  "some name",
		value: "some value",
	}

	as.Equal("some value", <-out)
	close(done)
}

func TestTableLookup(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()
	err := updater.Update(&row{
		id:    "some id",
		name:  "some name",
		value: "some value",
	})
	as.Nil(err)

	lookup, err := node.TableLookup(tbl, "value",
		func(k string) string {
			return k
		},
	)
	as.NotNil(lookup)
	as.Nil(err)

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan string)

	lookup.Start(context.Make(done, make(chan context.Advice), in, out))
	in <- "some id"
	as.Equal("some value", <-out)
	close(done)
}

func TestLookupCreateError(t *testing.T) {
	as := assert.New(t)

	tbl, _ := caravan.NewTable[string, any]("not-missing")
	lookup, err := node.TableLookup(tbl, "missing",
		func(_ any) string {
			return ""
		},
	)
	as.Nil(lookup)
	as.EqualError(err, fmt.Sprintf(table.ErrColumnNotFound, "missing"))
}

func TestLookupProcessError(t *testing.T) {
	as := assert.New(t)

	theKey := "the key"
	tbl, _ := caravan.NewTable[string, any]("*")

	lookup, e := node.TableLookup(tbl, "*",
		func(e any) string {
			return theKey
		},
	)

	as.NotNil(lookup)
	as.Nil(e)

	done := make(chan context.Done)
	in := make(chan any)
	monitor := make(chan context.Advice)

	lookup.Start(context.Make(done, monitor, in, make(chan any)))

	in <- "missing"
	as.EqualError(
		(<-monitor).(error), fmt.Sprintf(table.ErrKeyNotFound, theKey),
	)
	close(done)
}

func TestTableScan(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()

	// Populate table with multiple entries
	updater.Update(&row{id: "id1", name: "name1", value: "value1"})
	updater.Update(&row{id: "id2", name: "name2", value: "value2"})
	updater.Update(&row{id: "id3", name: "name3", value: "value3"})

	// Scanner that returns multiple keys
	scanner, err := node.TableScan(tbl, "value",
		func(keys []string) []string {
			return keys
		},
	)
	as.NotNil(scanner)
	as.Nil(err)

	done := make(chan context.Done)
	in := make(chan []string)
	out := make(chan string)

	scanner.Start(context.Make(done, make(chan context.Advice), in, out))

	// Send multiple keys to lookup
	in <- []string{"id1", "id2"}

	// Should receive both values
	result1 := <-out
	result2 := <-out
	results := []string{result1, result2}
	as.Contains(results, "value1")
	as.Contains(results, "value2")

	close(done)
}

func TestTableBatchUpdate(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()
	batchUpdater := node.TableBatchUpdate(updater)

	done := make(chan context.Done)
	in := make(chan []*row)
	out := make(chan []*row)

	batchUpdater.Start(context.Make(done, make(chan context.Advice), in, out))

	// Send batch of rows
	batch := []*row{
		{id: "id1", name: "name1", value: "value1"},
		{id: "id2", name: "name2", value: "value2"},
	}
	in <- batch

	// Should receive same batch after update
	result := <-out
	as.Equal(batch, result)

	// Verify table was updated
	getter, _ := tbl.Getter("value")
	val, err := getter("id1")
	as.Nil(err)
	as.Equal("value1", val[0])

	close(done)
}

func TestTableAggregate(t *testing.T) {
	as := assert.New(t)

	tbl, _ := caravan.NewTable[string, string]("count")
	setter, _ := tbl.Setter("count")

	aggregator := node.TableAggregate(
		0, // initial count
		func(count int, msg string) int {
			return count + 1
		},
		func(count int) (string, []string) {
			return "total", []string{fmt.Sprintf("%d", count)}
		},
		setter,
	)

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan int)

	aggregator.Start(context.Make(done, make(chan context.Advice), in, out))

	// Send messages and collect running totals
	in <- "msg1"
	as.Equal(1, <-out)

	in <- "msg2"
	as.Equal(2, <-out)

	in <- "msg3"
	as.Equal(3, <-out)

	// Verify table has final count
	getter, _ := tbl.Getter("count")
	val, err := getter("total")
	as.Nil(err)
	as.Equal("3", val[0])

	close(done)
}

func TestTableFilter(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()

	updater.Update(&row{id: "id1", name: "name1", value: "value1"})
	updater.Update(&row{id: "id2", name: "name2", value: "value2"})

	filter := node.TableFilter(tbl, func(id string) string {
		return id
	})

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan string)

	filter.Start(context.Make(done, make(chan context.Advice), in, out))

	go func() {
		in <- "id1"
		in <- "id_missing"
		in <- "id2"
	}()

	as.Equal("id1", <-out)
	as.Equal("id2", <-out)

	close(done)
}

func TestTableJoin(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()

	updater.Update(&row{id: "id1", name: "name1", value: "value1"})
	updater.Update(&row{id: "id2", name: "name2", value: "value2"})

	type enriched struct {
		id    string
		name  string
		value string
	}

	joiner, err := node.TableJoin(
		tbl,
		[]table.ColumnName{"name", "value"},
		func(id string) string {
			return id
		},
		func(id string, vals []string) enriched {
			return enriched{
				id:    id,
				name:  vals[0],
				value: vals[1],
			}
		},
	)
	as.Nil(err)

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan enriched)

	joiner.Start(context.Make(done, make(chan context.Advice), in, out))

	go func() {
		in <- "id1"
		in <- "id2"
	}()

	result1 := <-out
	as.Equal("id1", result1.id)
	as.Equal("name1", result1.name)
	as.Equal("value1", result1.value)

	result2 := <-out
	as.Equal("id2", result2.id)
	as.Equal("name2", result2.name)
	as.Equal("value2", result2.value)

	close(done)
}

func TestTableDelete(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()

	updater.Update(&row{id: "id1", name: "name1", value: "value1"})
	updater.Update(&row{id: "id2", name: "name2", value: "value2"})

	deleter := node.TableDelete(tbl, func(id string) string {
		return id
	})

	done := make(chan context.Done)
	in := make(chan string)
	out := make(chan string)

	deleter.Start(context.Make(done, make(chan context.Advice), in, out))

	go func() {
		in <- "id1"
	}()

	as.Equal("id1", <-out)

	getter, _ := tbl.Getter("value")
	_, err := getter("id1")
	as.EqualError(err, fmt.Sprintf(table.ErrKeyNotFound, "id1"))

	_, err = getter("id2")
	as.Nil(err)

	close(done)
}

func TestTableWatch(t *testing.T) {
	as := assert.New(t)

	tbl, updater := makeTestTable()
	updates := make(chan string, 10)

	updater.Update(&row{id: "id1", name: "name1", value: "value1"})
	updater.Update(&row{id: "id2", name: "name2", value: "value2"})

	watcher, err := node.TableWatch(tbl, updates, []table.ColumnName{"name", "value"})
	as.Nil(err)

	done := make(chan context.Done)
	in := make(chan stream.Source)
	out := make(chan []string)

	watcher.Start(context.Make(done, make(chan context.Advice), in, out))

	updates <- "id1"
	result := <-out
	as.Equal([]string{"name1", "value1"}, result)

	updates <- "id2"
	result = <-out
	as.Equal([]string{"name2", "value2"}, result)

	close(done)
}
