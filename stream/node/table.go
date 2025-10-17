package node

import (
	"github.com/kode4food/caravan/stream"
	"github.com/kode4food/caravan/stream/context"
	"github.com/kode4food/caravan/table"
)

// TableLookup performs a lookup on a table using the provided message. The Key
// extracts a Key from this message and uses it to perform the lookup against
// the Table. The Column returned by the lookup is forwarded to the next
// Processor
func TableLookup[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value], col table.ColumnName,
	key table.KeySelector[Msg, Key],
) (stream.Processor[Msg, Value], error) {
	get, err := tbl.Getter(col)
	if err != nil {
		return nil, err
	}
	return func(c *context.Context[Msg, Value]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			res, e := get(key(msg))
			if e != nil {
				if c.Error(e) {
					continue
				}
				return
			}

			if c.ForwardResult(res[0]) {
				continue
			}
			return
		}
	}, nil
}

// TableUpdater constructs a processor that sends all messages it sees to the
// provided table Updater
func TableUpdater[Msg any, Key comparable, Value any](
	t table.Updater[Msg, Key, Value],
) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			e := t.Update(msg)
			if e != nil {
				if c.Error(e) {
					continue
				}
				return
			}

			if c.ForwardResult(msg) {
				continue
			}
			return
		}
	}
}

// TableScan performs multiple lookups on a table using keys extracted from
// each message. Emits all found values for each message
func TableScan[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value], col table.ColumnName, fn func(Msg) []Key,
) (stream.Processor[Msg, Value], error) {
	get, err := tbl.Getter(col)
	if err != nil {
		return nil, err
	}
	return func(c *context.Context[Msg, Value]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			keys := fn(msg)
			allProcessed := true
			for _, key := range keys {
				res, e := get(key)
				if e != nil {
					if !c.Error(e) {
						allProcessed = false
						break
					}
					continue
				}
				if !c.ForwardResult(res[0]) {
					allProcessed = false
					break
				}
			}
			if allProcessed {
				continue
			}
			return
		}
	}, nil
}

// TableBatchUpdate constructs a processor that batches messages before
// updating the table, improving efficiency for high-throughput scenarios
func TableBatchUpdate[Msg any, Key comparable, Value any](
	t table.Updater[Msg, Key, Value],
) stream.Processor[[]Msg, []Msg] {
	return func(c *context.Context[[]Msg, []Msg]) {
		for {
			batch, ok := c.FetchMessage()
			if !ok {
				return
			}
			for _, msg := range batch {
				if e := t.Update(msg); e != nil && !c.Error(e) {
					return
				}
			}
			if !c.ForwardResult(batch) {
				return
			}
		}
	}
}

// TableAggregate maintains a running aggregation that updates a table.
// Combines Scan with TableUpdater to create materialized aggregations
func TableAggregate[Msg any, Agg any, Key comparable, Value any](
	init Agg, fn Reducer[Agg, Msg], toRow func(Agg) (Key, []Value),
	set table.Setter[Key, Value],
) stream.Processor[Msg, Agg] {
	return func(c *context.Context[Msg, Agg]) {
		agg := init
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			agg = fn(agg, msg)

			key, values := toRow(agg)
			if e := set(key, values...); e != nil && !c.Error(e) {
				return
			}

			if c.ForwardResult(agg) {
				continue
			}
			return
		}
	}
}

// TableFilter filters messages based on whether they exist in the table.
// Messages are forwarded only if the key lookup succeeds
func TableFilter[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value], key table.KeySelector[Msg, Key],
) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		columns := tbl.Columns()
		if len(columns) == 0 {
			return
		}
		get, err := tbl.Getter(columns[0])
		if err != nil {
			c.Error(err)
			return
		}

		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			k := key(msg)
			if _, e := get(k); e != nil {
				continue
			}
			if c.ForwardResult(msg) {
				continue
			}
			return
		}
	}
}

// TableJoin enriches stream messages with table data by performing a lookup
// and combining the message with the looked-up values using the provided join
// function
func TableJoin[Msg any, Key comparable, Value any, Out any](
	tbl table.Table[Key, Value], cols []table.ColumnName,
	key table.KeySelector[Msg, Key], fn func(Msg, []Value) Out,
) (stream.Processor[Msg, Out], error) {
	get, err := tbl.Getter(cols...)
	if err != nil {
		return nil, err
	}

	return func(c *context.Context[Msg, Out]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			k := key(msg)
			values, e := get(k)
			if e != nil {
				if c.Error(e) {
					continue
				}
				return
			}

			result := fn(msg, values)
			if c.ForwardResult(result) {
				continue
			}
			return
		}
	}, nil
}

// TableDelete removes rows from the table based on keys extracted from stream
// messages
func TableDelete[Msg any, Key comparable, Value any](
	tbl table.Table[Key, Value], key table.KeySelector[Msg, Key],
) stream.Processor[Msg, Msg] {
	return func(c *context.Context[Msg, Msg]) {
		for {
			msg, ok := c.FetchMessage()
			if !ok {
				return
			}

			k := key(msg)
			if e := tbl.Delete(k); e != nil {
				if c.Error(e) {
					continue
				}
				return
			}

			if c.ForwardResult(msg) {
				continue
			}
			return
		}
	}
}

// TableWatch emits table row data whenever the table is updated.
// It requires a channel that signals when updates occur
func TableWatch[Key comparable, Value any](
	tbl table.Table[Key, Value], updates <-chan Key, cols []table.ColumnName,
) (stream.Processor[stream.Source, []Value], error) {
	get, err := tbl.Getter(cols...)
	if err != nil {
		return nil, err
	}

	return func(c *context.Context[stream.Source, []Value]) {
		for {
			select {
			case <-c.Done:
				return
			case key, ok := <-updates:
				if !ok {
					return
				}
				if values, e := get(key); e == nil {
					if !c.ForwardResult(values) {
						return
					}
				}
			}
		}
	}, nil
}
