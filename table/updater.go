package table

type (
	Updater[Msg any, Key comparable, Value any] interface {
		// Key returns the key selector for this Table. This selector can be
		// used to retrieve the Key from a message for this Updater independent
		// of calls to Update
		Key() KeySelector[Msg, Key]

		// Columns will return the column selector definitions that are defined
		// as part of this Updater. These selectors can be used to retrieve
		// Values from a message for this Updater independent of calls to
		// Update
		Columns() []Column[Msg, Value]

		// Update extracts a Key and Column Values from a message and updates
		// the associated Table
		Update(Msg) error
	}

	// KeySelector is used to extract a Key from a message
	KeySelector[Msg any, Key comparable] func(Msg) Key

	// ValueSelector is used to extract a Value from a message
	ValueSelector[Msg, Value any] func(Msg) Value
)
