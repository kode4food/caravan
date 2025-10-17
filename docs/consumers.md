# Consumers

Consumers allow you to receive messages from the Log. You can do this using functions like `Poll`, which allows for a timeout, using `Receive`, which will block indefinitely, or retrieving the underlying channel and pulling messages directly from it.

Each Consumer instantiated from a Topic maintains an independent index into its Log. In this way, a Caravan Topic acts a bit like a Fanout Exchange in a Message Broker. The difference between Caravan and a Message Broker is that, in Caravan, Consumers can be instantiated at any time, and they will start consuming at the first retained message in the Log, even if that was the first message ever produced. In a Message Broker, you only have access to Messages produced after your Queue has been plumbed into the Topic.

## Poll

The `Poll` function allows the developer to query the Consumer for pending messages. If no message is immediately available, the call will block up to the specified amount of time.

```go
package main

import (
    "fmt"
    "time"

    "github.com/kode4food/caravan"
	"github.com/kode4food/caravan/message"
)

func main() {
    top := caravan.NewTopic[any]()
    // ... Code that produces messages
    c := top.NewConsumer()
    if e, ok := message.Poll(c, time.Millisecond * 50); ok {
        fmt.Println("Received: ", e)
    } else {
        fmt.Println("Nothing Received")
    }
}
```

## Receive

The `Receive` function will block indefinitely until a message is returned. If the Consumer has been closed, this boolean result will be `false`.

```go
package main

import (
    "fmt"

    "github.com/kode4food/caravan"
    "github.com/kode4food/caravan/message"
)

func main() {
    top := caravan.NewTopic[any]()
    // ... Code that produces messages
    c := top.NewConsumer()
    if e, ok := message.Receive(c); ok {
        fmt.Println("Received: ", e)
    } else {
        fmt.Println("Consumer was closed!")
    }
}
```
