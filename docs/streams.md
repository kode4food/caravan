# Stream APIs

Streams provide a mechanism for routing Caravan messages through a processing workflow. This generally entails consuming from a Topic, performing some form of transformation or filtering of messages, and then sinking the results into another Topic, but it doesn't have to end there.

For example, the following Stream filters out integers that are odd.

```go
package main

import (
    "fmt"

    "github.com/kode4food/caravan"
    "github.com/kode4food/caravan/stream/node"
    "github.com/kode4food/caravan/topic/config"
)

func main() {
    in := caravan.NewTopic[int](config.Consumed)
    out := caravan.NewTopic[int](config.Permanent)

    s := caravan.NewStream(
        // Stream from the 'in' topic
        node.TopicConsumer(in),
        // Filter messages coming from 'in' to only include
        // even numbers
        node.Filter(func(e int) bool {
            return e % 2 == 0
        }),
        // Stream the remaining messages to the 'out' topic
        node.TopicProducer(out),
    )
    _ = s.Start()

    go func() {
        c := out.NewConsumer()
        for i := range c.Receive() {
            fmt.Println(i)
        }
        c.Close()
    }()

    go func() {
        p := in.NewProducer()
        for i := 0; i < 100; i++ {
			p.Send() <- i
        }
        p.Close()
    }()

    <- make(chan bool) // hit ctrl-c
}
```
