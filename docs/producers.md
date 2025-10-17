# Producers

Producers allow you to append messages to the Log. The interface is rather simple. You can either use the `Send` method or retrieve the underlying `Channel` and push messages directly to it.

```go
package main

import (
    "fmt"

    "github.com/kode4food/caravan"
)

func main() {
    top := caravan.NewTopic[string]()
    p := top.NewProducer()
    select {
    case p.Send() <- "someMessage":
        fmt.Println("Send was successful")
    default:
        fmt.Println("Producer was closed!")
    }
}
```
