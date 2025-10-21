# Caravan Debugging

Debugging streaming apps can be difficult, that's why Caravan provides some basic debugging facilities, including the ability to log useful information to `stderr`. In order to enable this capability, set the `CARAVAN_DEBUG` environment variable:

```bash
export CARAVAN_DEBUG=1
```

This would be equivalent to performing the following calls somewhere in your code:

```go
package main

import (
    "os"

    "github.com/caravan/caravan/debug"
)

func main() {
    debug.Enable()
    debug.TailLogTo(os.Stderr)
}
```

## Consuming Debug Information

There are also in-process facilities that the programmer can leverage, such as using the `WithConsumer` function to capture debugging information as it is being emitted. For example, the following code enables all debug information logging and then prints any errors to the screen.

```go
package main

import (
    "fmt"
    "runtime"

    "github.com/kode4food/caravan"
    "github.com/kode4food/caravan/debug"
    "github.com/kode4food/caravan/topic"
)

func main() {
    debug.Enable()
    go func() {
        debug.WithConsumer(func (c topic.Consumer[error]) {
            for e := range c.Receive() {
                fmt.Println(e)
            }
        })
    }()
    caravan.NewTopic[string]().NewConsumer()
    // This garbage collection call will trigger a debug
    // message related to not explicitly closing a Consumer
    runtime.GC()
    <- make(chan bool) // hit ctrl-c
}
```
