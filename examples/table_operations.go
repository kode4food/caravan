package main

import (
	"fmt"
	"time"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/stream/node"
	"github.com/kode4food/caravan/table"
)

// Example: Real-time order processing with table operations

type Order struct {
	ID     string
	UserID string
	Amount int
	Status string
}

type UserStats struct {
	UserID      string
	OrderCount  int
	TotalAmount int
}

func main() {
	fmt.Println("=== Table Operations Example ===")

	// Example 1: Batch Updates for High Throughput
	example1BatchUpdates()

	// Example 2: Table Scan for Multi-key Lookups
	example2TableScan()

	// Example 3: Running Aggregations with TableAggregate
	example3Aggregations()

	// Example 4: Combining operators for complex workflows
	example4ComplexWorkflow()
}

func example1BatchUpdates() {
	fmt.Println("Example 1: Batch Table Updates")
	fmt.Println("--------------------------------")

	// Create table and updater for orders
	ordersTable, _ := caravan.NewTable[string, string](
		"id", "user_id", "amount", "status",
	)

	orderUpdater, _ := caravan.NewTableUpdater(
		ordersTable,
		func(o *Order) string { return o.ID },
		table.MakeColumn("id",
			func(o *Order) string { return o.ID },
		),
		table.MakeColumn("user_id",
			func(o *Order) string { return o.UserID },
		),
		table.MakeColumn("amount",
			func(o *Order) string { return fmt.Sprintf("%d", o.Amount) },
		),
		table.MakeColumn("status",
			func(o *Order) string { return o.Status },
		),
	)

	// Create topics
	ordersIn := caravan.NewTopic[*Order]()
	batchesOut := caravan.NewTopic[[]*Order]()

	// Stream: Buffer orders then batch update table
	s := caravan.NewStream(
		node.Bind(
			node.Bind(
				node.TopicConsumer(ordersIn),
				node.Buffer[*Order](10, 500*time.Millisecond), // Batch 10 or 500ms
			),
			node.TableBatchUpdate(orderUpdater),
		),
		node.TopicProducer(batchesOut),
	)

	running := s.Start()
	defer running.Stop()

	// Send orders
	go func() {
		p := ordersIn.NewProducer()
		defer p.Close()
		for i := 0; i < 25; i++ {
			p.Send() <- &Order{
				ID:     fmt.Sprintf("order-%d", i),
				UserID: fmt.Sprintf("user-%d", i%5),
				Amount: (i + 1) * 100,
				Status: "pending",
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Consume batches
	c := batchesOut.NewConsumer()
	defer c.Close()

	batchCount := 0
	totalOrders := 0

	timeout := time.After(3 * time.Second)
	for {
		select {
		case batch := <-c.Receive():
			batchCount++
			totalOrders += len(batch)
			fmt.Printf("Batch %d: Updated %d orders\n", batchCount, len(batch))
		case <-timeout:
			fmt.Printf("Total: %d batches, %d orders\n\n",
				batchCount, totalOrders,
			)
			return
		}
	}
}

func example2TableScan() {
	fmt.Println("Example 2: Table Scan for Multi-key Lookups")
	fmt.Println("--------------------------------------------")

	// Create and populate orders table
	ordersTable, _ := caravan.NewTable[string, string]("user_id", "amount")
	setter, _ := ordersTable.Setter("user_id", "amount")

	// Populate with sample data
	setter("order-1", "user-1", "100")
	setter("order-2", "user-1", "200")
	setter("order-3", "user-2", "150")
	setter("order-4", "user-2", "250")

	// Create topics
	lookupRequestsIn := caravan.NewTopic[[]string]()
	amountsOut := caravan.NewTopic[string]()

	// TableScan processor
	scanner, _ := node.TableScan(
		ordersTable,
		"amount",
		func(keys []string) []string { return keys },
	)

	// Stream: Scan multiple orders at once
	s := caravan.NewStream(
		node.Bind(
			node.TopicConsumer(lookupRequestsIn),
			scanner,
		),
		node.TopicProducer(amountsOut),
	)

	running := s.Start()
	defer running.Stop()

	// Request multiple order amounts
	go func() {
		p := lookupRequestsIn.NewProducer()
		defer p.Close()
		p.Send() <- []string{"order-1", "order-2", "order-3"}
	}()

	// Collect results
	c := amountsOut.NewConsumer()
	defer c.Close()

	var results []string
	timeout := time.After(500 * time.Millisecond)
	for len(results) < 3 {
		select {
		case amount := <-c.Receive():
			results = append(results, amount)
			fmt.Printf("Found amount: %s\n", amount)
		case <-timeout:
			fmt.Println("Timeout waiting for results")
			return
		}
	}
	fmt.Printf("Retrieved %d amounts\n\n", len(results))
}

func example3Aggregations() {
	fmt.Println("Example 3: Running Aggregations with TableAggregate")
	fmt.Println("----------------------------------------------------")

	// Create table for aggregated stats
	statsTable, _ := caravan.NewTable[string, string](
		"user_id", "order_count", "total_amount",
	)
	setter, _ := statsTable.Setter("user_id", "order_count", "total_amount")

	// Create topics
	ordersIn := caravan.NewTopic[*Order]()
	statsOut := caravan.NewTopic[*UserStats]()

	// Aggregate: Count orders and sum amounts
	aggregator := node.TableAggregate(
		&UserStats{UserID: "user-1", OrderCount: 0, TotalAmount: 0},
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
	)

	// Stream: Aggregate orders and update stats table
	s := caravan.NewStream(
		node.Bind(
			node.Bind(
				node.TopicConsumer(ordersIn),
				node.Filter(
					func(o *Order) bool { return o.UserID == "user-1" },
				),
			),
			aggregator,
		),
		node.TopicProducer(statsOut),
	)

	running := s.Start()
	defer running.Stop()

	// Send orders
	go func() {
		p := ordersIn.NewProducer()
		defer p.Close()
		orders := []*Order{
			{ID: "order-1", UserID: "user-1", Amount: 100, Status: "pending"},
			{ID: "order-2", UserID: "user-2", Amount: 200, Status: "pending"},
			{ID: "order-3", UserID: "user-1", Amount: 150, Status: "pending"},
			{ID: "order-4", UserID: "user-1", Amount: 300, Status: "pending"},
		}
		for _, order := range orders {
			p.Send() <- order
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Collect running stats
	c := statsOut.NewConsumer()
	defer c.Close()

	timeout := time.After(1 * time.Second)
	for {
		select {
		case stats := <-c.Receive():
			fmt.Printf("User %s: %d orders, $%d total\n",
				stats.UserID, stats.OrderCount, stats.TotalAmount)
		case <-timeout:
			// Check final table state
			getter, _ := statsTable.Getter("order_count", "total_amount")
			vals, _ := getter("user-1")
			fmt.Printf("\nFinal table state - Orders: %s, Total: $%s\n\n",
				vals[0], vals[1],
			)
			return
		}
	}
}

func example4ComplexWorkflow() {
	fmt.Println("Example 4: Complex Workflow - Dedupe + Batching + Aggregation")
	fmt.Println("-------------------------------------------------------------")

	// Create tables
	ordersTable, _ := caravan.NewTable[string, string](
		"id", "user_id", "amount",
	)
	statsTable, _ := caravan.NewTable[string, string]("user_id", "total")
	statsSetter, _ := statsTable.Setter("user_id", "total")

	orderUpdater, _ := caravan.NewTableUpdater(
		ordersTable,
		func(o *Order) string { return o.ID },
		table.MakeColumn("id",
			func(o *Order) string { return o.ID },
		),
		table.MakeColumn("user_id",
			func(o *Order) string { return o.UserID },
		),
		table.MakeColumn("amount",
			func(o *Order) string { return fmt.Sprintf("%d", o.Amount) },
		),
	)

	// Create topics
	ordersIn := caravan.NewTopic[*Order]()
	statsOut := caravan.NewTopic[map[string]int]()

	// Complex pipeline:
	// 1. Deduplicate orders by ID
	// 2. Buffer for batch updates
	// 3. Update orders table in batches
	// 4. Flatten batches back to individual orders
	// 5. Scan to aggregate totals by user
	// 6. Update stats table
	s := caravan.NewStream(
		node.Bind(
			node.Bind(
				node.Bind(
					node.Bind(
						node.Bind(
							node.TopicConsumer(ordersIn),
							node.DistinctBy(
								func(o *Order) string { return o.ID },
							),
						),
						node.Buffer[*Order](5, 200*time.Millisecond),
					),
					node.TableBatchUpdate(orderUpdater),
				),
				node.FlatMap(func(batch []*Order) []*Order { return batch }),
			),
			node.Scan(
				func(totals map[string]int, o *Order) map[string]int {
					totals[o.UserID] += o.Amount
					// Update stats table
					statsSetter(
						o.UserID, o.UserID, fmt.Sprintf("%d", totals[o.UserID]),
					)
					return totals
				},
			),
		),
		node.TopicProducer(statsOut),
	)

	running := s.Start()
	defer running.Stop()

	// Send orders with some duplicates
	go func() {
		p := ordersIn.NewProducer()
		defer p.Close()
		orders := []*Order{
			{ID: "order-1", UserID: "alice", Amount: 100, Status: "pending"},
			{ID: "order-1", UserID: "alice", Amount: 100, Status: "pending"}, // duplicate
			{ID: "order-2", UserID: "bob", Amount: 200, Status: "pending"},
			{ID: "order-3", UserID: "alice", Amount: 150, Status: "pending"},
			{ID: "order-2", UserID: "bob", Amount: 200, Status: "pending"}, // duplicate
			{ID: "order-4", UserID: "bob", Amount: 300, Status: "pending"},
		}
		for _, order := range orders {
			p.Send() <- order
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Monitor aggregations
	c := statsOut.NewConsumer()
	defer c.Close()

	timeout := time.After(1 * time.Second)
	lastTotals := map[string]int{}
	for {
		select {
		case totals := <-c.Receive():
			lastTotals = totals
			fmt.Printf("Running totals: %v\n", totals)
		case <-timeout:
			fmt.Printf("\nFinal totals: alice=$%d, bob=$%d\n",
				lastTotals["alice"], lastTotals["bob"])
			fmt.Println(
				"\nDemonstrated: dedupe, batching, table updates, aggregation",
			)
			return
		}
	}
}
