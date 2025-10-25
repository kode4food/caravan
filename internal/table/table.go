package table

import (
	"fmt"
	"sync"

	"github.com/kode4food/caravan/table"
)

// Table is the internal implementation of a table.Table
type Table[Key comparable, Value any] struct {
	indexes map[table.ColumnName]int
	rows    map[Key][]Value
	names   []table.ColumnName
	mu      sync.RWMutex
}

func Make[Key comparable, Value any](
	cols ...table.ColumnName,
) (table.Table[Key, Value], error) {
	if err := checkColumnDuplicates(cols); err != nil {
		return nil, err
	}
	indexes := map[table.ColumnName]int{}
	for i, n := range cols {
		indexes[n] = i
	}
	return &Table[Key, Value]{
		names:   cols,
		indexes: indexes,
		rows:    map[Key][]Value{},
	}, nil
}

func (t *Table[_, _]) Columns() []table.ColumnName {
	return t.names[:]
}

func (t *Table[Key, Value]) Getter(
	cols ...table.ColumnName,
) (table.Getter[Key, Value], error) {
	indexes, err := t.columnIndexes(cols)
	if err != nil {
		return nil, err
	}
	return func(k Key) ([]Value, error) {
		t.mu.RLock()
		defer t.mu.RUnlock()

		if e, ok := t.rows[k]; ok {
			res := make([]Value, len(indexes))
			for out, in := range indexes {
				res[out] = e[in]
			}
			return res, nil
		}
		return nil, fmt.Errorf("%w: %v", table.ErrKeyNotFound, k)
	}, nil
}

func (t *Table[Key, Value]) Setter(
	cols ...table.ColumnName,
) (table.Setter[Key, Value], error) {
	indexes, err := t.columnIndexes(cols)
	if err != nil {
		return nil, err
	}
	if err := checkColumnDuplicates(cols); err != nil {
		return nil, err
	}

	return func(k Key, v ...Value) error {
		t.mu.Lock()
		defer t.mu.Unlock()

		if len(v) != len(indexes) {
			return fmt.Errorf("%w: need %d, got %d",
				table.ErrValueCountRequired, len(indexes), len(v),
			)
		}
		e, ok := t.rows[k]
		if !ok {
			e = make([]Value, len(t.names))
		}
		for in, out := range indexes {
			e[out] = v[in]
		}
		t.rows[k] = e
		return nil
	}, nil
}

func (t *Table[_, _]) columnIndexes(c []table.ColumnName) ([]int, error) {
	sel := make([]int, len(c))
	for i, name := range c {
		if s, ok := t.indexes[name]; ok {
			sel[i] = s
			continue
		}
		return nil, fmt.Errorf("%w: %s", table.ErrColumnNotFound, name)
	}
	return sel, nil
}

func (t *Table[Key, Value]) Delete(k Key) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.rows[k]; !ok {
		return fmt.Errorf("%w: %v", table.ErrKeyNotFoundDelete, k)
	}
	delete(t.rows, k)
	return nil
}

func (t *Table[Key, Value]) Keys() []Key {
	t.mu.RLock()
	defer t.mu.RUnlock()

	keys := make([]Key, 0, len(t.rows))
	for k := range t.rows {
		keys = append(keys, k)
	}
	return keys
}

func (t *Table[_, _]) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.rows)
}

func (t *Table[Key, Value]) Range(fn func(Key, []Value) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for k, v := range t.rows {
		// Make a copy of the row to avoid holding the lock during callback
		rowCopy := make([]Value, len(v))
		copy(rowCopy, v)
		if !fn(k, rowCopy) {
			return
		}
	}
}

func checkColumnDuplicates(c []table.ColumnName) error {
	names := map[table.ColumnName]bool{}
	for _, n := range c {
		if _, ok := names[n]; ok {
			return fmt.Errorf("%w: %s", table.ErrDuplicateColumnName, n)
		}
		names[n] = true
	}
	return nil
}
