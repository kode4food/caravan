package table

import (
	"fmt"
	"sync"

	"github.com/kode4food/caravan/table"
)

type store[Key comparable, Value any] struct {
	indexes map[table.ColumnName]int
	rows    map[Key][]Value
	names   []table.ColumnName
	mu      sync.RWMutex
}

// New returns an empty Table with the provided columns
func New[Key comparable, Value any](
	cols ...table.ColumnName,
) (table.Table[Key, Value], error) {
	if err := checkColumnDuplicates(cols); err != nil {
		return nil, err
	}
	indexes := map[table.ColumnName]int{}
	for i, n := range cols {
		indexes[n] = i
	}
	return &store[Key, Value]{
		names:   cols,
		indexes: indexes,
		rows:    map[Key][]Value{},
	}, nil
}

// Columns returns the names defined by the Table
func (s *store[_, _]) Columns() []table.ColumnName {
	return s.names[:]
}

// Getter returns a row reader for the selected Columns
func (s *store[Key, Value]) Getter(
	cols ...table.ColumnName,
) (table.Getter[Key, Value], error) {
	indexes, err := s.columnIndexes(cols)
	if err != nil {
		return nil, err
	}
	return func(k Key) ([]Value, error) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		if e, ok := s.rows[k]; ok {
			res := make([]Value, len(indexes))
			for out, in := range indexes {
				res[out] = e[in]
			}
			return res, nil
		}
		return nil, fmt.Errorf("%w: %v", table.ErrKeyNotFound, k)
	}, nil
}

// Setter returns a row writer for the selected Columns
func (s *store[Key, Value]) Setter(
	cols ...table.ColumnName,
) (table.Setter[Key, Value], error) {
	indexes, err := s.columnIndexes(cols)
	if err != nil {
		return nil, err
	}
	if err := checkColumnDuplicates(cols); err != nil {
		return nil, err
	}

	return func(k Key, v ...Value) error {
		s.mu.Lock()
		defer s.mu.Unlock()

		if len(v) != len(indexes) {
			return fmt.Errorf("%w: need %d, got %d",
				table.ErrValueCountRequired, len(indexes), len(v),
			)
		}
		e, ok := s.rows[k]
		if !ok {
			e = make([]Value, len(s.names))
		}
		for in, out := range indexes {
			e[out] = v[in]
		}
		s.rows[k] = e
		return nil
	}, nil
}

func (s *store[_, _]) columnIndexes(c []table.ColumnName) ([]int, error) {
	sel := make([]int, len(c))
	for i, name := range c {
		if index, ok := s.indexes[name]; ok {
			sel[i] = index
			continue
		}
		return nil, fmt.Errorf("%w: %s", table.ErrColumnNotFound, name)
	}
	return sel, nil
}

// Delete removes a row by key
func (s *store[Key, Value]) Delete(k Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.rows[k]; !ok {
		return fmt.Errorf("%w: %v", table.ErrKeyNotFoundDelete, k)
	}
	delete(s.rows, k)
	return nil
}

// Keys returns every key currently stored in the Table
func (s *store[Key, Value]) Keys() []Key {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]Key, 0, len(s.rows))
	for k := range s.rows {
		keys = append(keys, k)
	}
	return keys
}

// Count returns the number of rows in the Table
func (s *store[_, _]) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// Range visits rows until fn returns false
func (s *store[Key, Value]) Range(fn func(Key, []Value) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.rows {
		// Keep callbacks from mutating stored rows
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
