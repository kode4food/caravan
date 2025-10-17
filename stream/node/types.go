package node

// Common function signature types for stream processing operations.
// These types make function purposes self-documenting and ensure consistency
// across the API.

type (
	// Predicate tests if a message meets a condition. Returning false will
	// drop the message from the stream
	Predicate[Msg any] func(Msg) bool

	// Mapper transforms a message from one type to another. The returned
	// message is forwarded downstream
	Mapper[From, To any] func(From) To

	// FlatMapper transforms one message into zero or more messages. Each
	// result is forwarded downstream in order
	FlatMapper[From, To any] func(From) []To

	// Reducer combines an accumulator with a message to produce a new
	// accumulator. Used for aggregation and folding operations
	Reducer[Res, Msg any] func(Res, Msg) Res

	// BinaryPredicate tests a condition on two messages. Returning true
	// indicates the messages should be combined (used for joins)
	BinaryPredicate[Left, Right any] func(Left, Right) bool

	// BinaryOperator combines two messages into a result. Used to merge
	// messages from multiple streams
	BinaryOperator[Left, Right, Out any] func(Left, Right) Out

	// Equality tests if two messages are equal. Used for deduplication
	Equality[Msg any] func(Msg, Msg) bool

	// KeySelector extracts a comparable key from a message. Used for
	// grouping and keyed operations
	KeySelector[Msg any, Key comparable] func(Msg) Key

	// Consumer performs a side effect on a message without modifying it.
	// The message is forwarded downstream after the action
	Consumer[Msg any] func(Msg)

	// Generator produces a message and indicates if generation should
	// continue. Returning false stops the generation
	Generator[Msg any] func() (Msg, bool)
)
