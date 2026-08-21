package caravan

import (
	"github.com/kode4food/caravan/internal/stream/node"
	"github.com/kode4food/caravan/table"
)

// TableLookup looks up one Table value for each message
func (p StreamPipeline[Msg]) TableLookup[Key comparable, Value any](
	tbl table.Table[Key, Value], col table.ColumnName,
	key table.KeySelector[Msg, Key],
) (StreamPipeline[Value], error) {
	processor, err := node.TableLookup(tbl, col, key)
	if err != nil {
		return StreamPipeline[Value]{}, err
	}
	return p.process(processor), nil
}

// TableScan looks up multiple Table values for each message
func (p StreamPipeline[Msg]) TableScan[Key comparable, Value any](
	tbl table.Table[Key, Value], col table.ColumnName, fn func(Msg) []Key,
) (StreamPipeline[Value], error) {
	processor, err := node.TableScan(tbl, col, fn)
	if err != nil {
		return StreamPipeline[Value]{}, err
	}
	return p.process(processor), nil
}

// TableUpdate updates a Table for each message
func (p StreamPipeline[Msg]) TableUpdate[Key comparable, Value any](
	updater table.Updater[Msg, Key, Value],
) StreamPipeline[Msg] {
	return p.process(node.TableUpdate(updater))
}

// TableAggregate maintains an aggregate in a Table
func (p StreamPipeline[Msg]) TableAggregate[
	Agg any, Key comparable, Value any,
](
	init Agg, fn func(Agg, Msg) Agg, toRow func(Agg) (Key, []Value),
	set table.Setter[Key, Value],
) StreamPipeline[Agg] {
	return p.process(node.TableAggregate(init, fn, toRow, set))
}

// TableFilter forwards messages whose keys exist in a Table
func (p StreamPipeline[Msg]) TableFilter[Key comparable, Value any](
	tbl table.Table[Key, Value], key table.KeySelector[Msg, Key],
) StreamPipeline[Msg] {
	return p.process(node.TableFilter(tbl, key))
}

// TableJoin joins messages with values from a Table
func (p StreamPipeline[Msg]) TableJoin[Key comparable, Value, Out any](
	tbl table.Table[Key, Value], cols []table.ColumnName,
	key table.KeySelector[Msg, Key], fn func(Msg, []Value) Out,
) (StreamPipeline[Out], error) {
	processor, err := node.TableJoin(tbl, cols, key, fn)
	if err != nil {
		return StreamPipeline[Out]{}, err
	}
	return p.process(processor), nil
}

// TableDelete deletes each message's key from a Table
func (p StreamPipeline[Msg]) TableDelete[Key comparable, Value any](
	tbl table.Table[Key, Value], key table.KeySelector[Msg, Key],
) StreamPipeline[Msg] {
	return p.process(node.TableDelete(tbl, key))
}

// TableWatch starts a Stream that watches a Table
func TableWatch[Key comparable, Value any](
	tbl table.Table[Key, Value], updates <-chan Key, cols []table.ColumnName,
) (StreamPipeline[[]Value], error) {
	processor, err := node.TableWatch(tbl, updates, cols)
	if err != nil {
		return StreamPipeline[[]Value]{}, err
	}
	return start(processor), nil
}
