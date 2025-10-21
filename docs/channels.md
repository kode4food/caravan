# Channel APIs

Channel APIs take the work of sending to a Producer, or receiving from a Consumer, and simplify the approach down to using [Go Channels](https://tour.golang.org/concurrency/2). In order to use these APIs, you'd call the `Receive` method on a Consumer, or the `Send` method on a Producer. For example:

```go
package main

import (
    "fmt"

    "github.com/kode4food/caravan"
)

func main() {
    top := caravan.NewTopic[string]()

    // Send some stuff to the topic via its channel
    go func() {
        p := top.NewProducer()
        for i := 0; i < 10; i++ {
            p.Send() <- fmt.Sprintf("Message %d", i)
        }
        p.Close()
    }()

    // Receive some stuff from the topic via its channel
    go func() {
        c := top.NewConsumer()
        for i := 0; i < 10; i++ {
            fmt.Println(<-c.Receive())
        }
        c.Close()
    }()

    <- make(chan bool) // hit ctrl-c
}
```

One difference between normal channel use in Go and the Channel API in Caravan is that you still have to explicitly call `Close` on the Producer or Consumer. The reason for this is that a Topic manages a set of subscriber resources under the hood that can't be automatically garbage collected if the channel is closed.
