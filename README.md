# Caravan <img src="./docs/caravan-logo.png" align="right" height="100"/>

### Stream Processing and State Management for Go

![Build Status](https://github.com/kode4food/caravan/actions/workflows/build.yml/badge.svg) [![Code Coverage](https://qlty.sh/gh/kode4food/projects/caravan/coverage.svg)](https://qlty.sh/gh/kode4food/projects/caravan) [![Maintainability](https://qlty.sh/gh/kode4food/projects/caravan/maintainability.svg)](https://qlty.sh/gh/kode4food/projects/caravan) [![GitHub](https://img.shields.io/github/license/kode4food/caravan)](https://github.com/kode4food/caravan/blob/main/LICENSE)

Caravan is a Go library that provides powerful primitives for building in-process streaming applications with integrated state management. It combines reactive stream processing with efficient table operations to enable real-time data transformations, aggregations, and stateful workflows.

_This is a work in progress. The basics are there, but not yet ready for production use. Use at your own risk_

## Features

- **Topics** - FIFO message logs with independent producer/consumer tracking
- **Streams** - Composable stream processing pipelines with rich operators
- **Tables** - In-memory key-value tables with column-based storage
- **Type Safety** - Fully generic APIs leveraging Go generics
- **Backpressure** - Natural flow control through channel semantics
- **Batch Operations** - Efficient batch updates for high-throughput scenarios
- **Aggregations** - Stateful stream aggregations with table integration

## Installation

Caravan requires Go 1.27 or later.

```bash
go get github.com/kode4food/caravan
```

## Quick Start

### Topics

Topics provide in-process FIFO message logs with independent consumer tracking:

```go
import "github.com/kode4food/caravan"

// Create a topic
topic := caravan.NewTopic[string]()

// Produce messages
producer := topic.NewProducer()
defer producer.Close()
producer.Send() <- "hello world"

// Consume messages
consumer := topic.NewConsumer()
defer consumer.Close()
msg := <-consumer.Receive()
```

### Streams

Build composable stream processing pipelines:

```go
import "github.com/kode4food/caravan"

type Order struct {
    ID     string
    Amount int
}

// Create input/output topics
ordersIn := caravan.NewTopic[*Order]()
totalsOut := caravan.NewTopic[int]()

// Build a stream pipeline
stream := caravan.TopicConsumer(ordersIn).
    Filter(func(o *Order) bool {
        return o.Amount > 100
    }).
    Map(func(o *Order) int {
        return o.Amount
    }).
    TopicProducer(totalsOut)

// Start processing
running := stream.Start()
defer running.Stop()
```

### Tables

Efficient key-value storage with column-based operations:

```go
import (
    "github.com/kode4food/caravan"
    "github.com/kode4food/caravan/table"
)

type User struct {
    ID    string
    Name  string
    Email string
}

// Create a table
usersTable, _ := caravan.NewTable[string, string](
    "id", "name", "email",
)

// Create an updater
updater, _ := caravan.NewTableUpdater(
    usersTable,
    func(u *User) string { return u.ID },
    table.NewColumn("id", func(u *User) string { return u.ID }),
    table.NewColumn("name", func(u *User) string { return u.Name }),
    table.NewColumn("email", func(u *User) string { return u.Email }),
)

// Update the table from a stream
usersIn := caravan.NewTopic[*User]()
usersOut := caravan.NewTopic[*User]()

stream := caravan.TopicConsumer(usersIn).
    TableUpdate(updater).
    TopicProducer(usersOut)

running := stream.Start()
defer running.Stop()

// Produce users
producer := usersIn.NewProducer()
defer producer.Close()
producer.Send() <- &User{ID: "1", Name: "Alice", Email: "alice@example.com"}

// Consume to allow processing
consumer := usersOut.NewConsumer()
defer consumer.Close()
<-consumer.Receive()

// Query the table
getter, _ := usersTable.Getter("name", "email")
values, _ := getter("1")
fmt.Printf("User: %s <%s>\n", values[0], values[1])
```

## Stream Operators

Caravan provides a rich set of stream operators for building complex pipelines:

### Transformation

- `Map` - Transform messages one-to-one
- `FlatMap` - Transform messages one-to-many
- `Filter` - Filter messages by predicate
- `ForEach` - Perform an action for each message

### Aggregation

- `Scan` - Stateful accumulation
- `ScanFrom` - Stateful accumulation from an initial value
- `TableAggregate` - Aggregate to table storage
- `Reduce` - Reduce to single value
- `ReduceFrom` - Reduce from an initial value

### Grouping & Windowing

- `GroupBy` - Partition by key
- `Window` - Time-based windowing
- `SlidingWindow` - Fixed-size sliding windowing
- `Buffer` - Batch messages by count or time

### Table Integration

- `TableUpdate` - Update table from stream
- `TableBatchUpdate` - Batch update table
- `TableLookup` - Single-key table lookups
- `TableScan` - Multi-key table lookups
- `TableAggregate` - Aggregate into table
- `TableFilter` - Filter by table membership
- `TableJoin` - Join messages with table rows
- `TableDelete` - Delete rows from a stream
- `TableWatch` - Stream table changes

### Flow Control

- `Take` / `TakeWhile` - Limit message count
- `Skip` / `SkipWhile` - Skip messages
- `Distinct` - Remove consecutive duplicates
- `DistinctBy` - Remove duplicates
- `Retry` - Retry failed operations
- `Debounce` - Rate limiting
- `Throttle` - Throttle message rate
- `Delay` - Delay messages
- `Sample` - Sample messages periodically

### Utilities

- `Split` - Send messages through multiple branches
- `Merge` - Merge multiple streams
- `Zip` / `ZipWith` - Combine paired messages
- `CombineLatest` - Combine the latest messages
- `Join` - Combine matching paired messages
- `Generate` - Generate messages
- `GenerateFrom` - Generate messages from a channel
- `SidechainTo` - Copy messages to a channel
- `SinkInto` - End a stream at a channel

## Advanced Examples

### Real-time Aggregation with Tables

```go
// Aggregate order totals in real-time
statsTable, _ := caravan.NewTable[string, string](
    "user_id", "order_count", "total_amount",
)
setter, _ := statsTable.Setter("user_id", "order_count", "total_amount")

ordersIn := caravan.NewTopic[*Order]()
statsOut := caravan.NewTopic[*UserStats]()

stream := caravan.TopicConsumer(ordersIn).
    TableAggregate(
        &UserStats{OrderCount: 0, TotalAmount: 0},
        func(stats *UserStats, order *Order) *UserStats {
            return &UserStats{
                UserID:      order.UserID,
                OrderCount:  stats.OrderCount + 1,
                TotalAmount: stats.TotalAmount + order.Amount,
            }
        },
        func(stats *UserStats) (string, []string) {
            return stats.UserID, []string{
                stats.UserID,
                fmt.Sprintf("%d", stats.OrderCount),
                fmt.Sprintf("%d", stats.TotalAmount),
            }
        },
        setter,
    ).
    TopicProducer(statsOut)
```

### Batch Processing for High Throughput

```go
// Buffer messages and update table in batches
ordersIn := caravan.NewTopic[*Order]()
batchesOut := caravan.NewTopic[[]*Order]()

stream := caravan.TopicConsumer(ordersIn).
    Buffer(100, 1*time.Second).
    TableBatchUpdate(orderUpdater).
    TopicProducer(batchesOut)
```

### Complex Multi-stage Pipeline

```go
// Deduplicate -> Batch -> Update -> Aggregate
stream := caravan.TopicConsumer(ordersIn).
    DistinctBy(func(o *Order) string {
        return o.ID
    }).
    Buffer(10, 500*time.Millisecond).
    TableBatchUpdate(orderUpdater).
	FlatMap(func(batch []*Order) []*Order {
		return batch
	}).
    Scan(func(totals map[string]int, o *Order) map[string]int {
        totals[o.UserID] += o.Amount
        return totals
    }).
    TopicProducer(statsOut)
```

## Error Handling

Streams provide flexible error handling through the advice system:

```go
running := stream.StartWith(func(advice context.Advice, next func()) {
    switch a := advice.(type) {
    case *context.Error:
        log.Printf("Recoverable error: %v", a)
        // Error is logged but processing continues
    case *context.Fatal:
        log.Printf("Fatal error: %v", a)
        _ = running.Stop() // Fatal stops the processor
    case *context.Debug:
        log.Printf("Debug: %v", a)
    case context.Stop:
        _ = running.Stop() // Stop advises processor to stop
    }
})
```
