# Caravan Documentation

Caravan is an in-process message streaming library for Go applications. Think Kafka, but for the internal workings of your software. With it, you can decouple the emission of observable messages from the act of consuming them. In Caravan, the messages that a producer generates may be consumed by hundreds of downstream consumers, or none at all.

## Caravan Basics

There are four basic concepts to wrap one's mind around when working with Caravan.

### messages

Messages that are passed around by Caravan. Caravan restricts the type per Topic using [Go Type Parameters](https://go.googlesource.com/proposal/+/refs/heads/master/design/go2draft-type-parameters.md).

### Topics

[Topics](./topics.md) are the nexus of message streaming in Caravan. They manage the Log and are how you instantiate Producers and Consumers.

### Producers

[Producers](./producers.md) allow you to append messages to the Log. The interface is rather simple. You can either use the `message.Send` function or retrieve the underlying channel using the Producer's `Send` method and push messages directly to it.

### Consumers

[Consumers](./consumers.md) allow you to receive messages from the Log. You can do this using function like `message.Poll`, which allows for a timeout, using `message.Receive`, which will block indefinitely, or retrieving the underlying channel using the `Receive` method and pulling messages directly from it.
