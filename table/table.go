package table

import "errors"

type (
	// Table associates a Key with multiple named Columns
	// The Key and Columns are selected using an Updater. Multiple Updaters are
	// able to act on a single Table
	Table[Key comparable, Value any] interface {
		// Columns will return the Column names defined for this Table
		Columns() []ColumnName

		// Getter creates a Getter for the specified ColumnNames
		Getter(...ColumnName) (Getter[Key, Value], error)

		// Setter creates a Setter for the specified ColumnNames
		Setter(...ColumnName) (Setter[Key, Value], error)

		// Delete removes a row from the table by key
		Delete(Key) error

		// Keys returns all keys currently in the table
		Keys() []Key

		// Count returns the number of rows in the table
		Count() int

		// Range visits rows until fn returns false
		Range(fn func(Key, []Value) bool)
	}

	// ColumnName is exactly what you think it is
	ColumnName string

	// Getter is a function that is capable of retrieving a pre-defined set of
	// column Values from a Table based on the provided Key
	Getter[Key comparable, Value any] func(Key) ([]Value, error)

	// Setter is a function that is capable of updating a pre-defined set of
	// column Values in a Table based on the provided Key
	Setter[Key comparable, Value any] func(Key, ...Value) error
)

var (
	// ErrKeyNotFound indicates a missing Table key
	ErrKeyNotFound = errors.New("key not found in table")
	// ErrColumnNotFound indicates a missing Table Column
	ErrColumnNotFound = errors.New("column not found in table")
	// ErrDuplicateColumnName indicates repeated Column names
	ErrDuplicateColumnName = errors.New("column name duplicated")
	// ErrValueCountRequired indicates an incorrect row value count
	ErrValueCountRequired = errors.New("value count mismatch")
	// ErrKeyNotFoundDelete indicates deletion of a missing key
	ErrKeyNotFoundDelete = errors.New("key not found for delete")
)
