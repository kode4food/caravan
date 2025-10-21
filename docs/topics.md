# Topics

Topics are the nexus of message streaming in Caravan. They manage the Log and are how you instantiate Producers and Consumers.

## The Log

Each Topic has an append-only Log. The Log automatically drops initial segments once all active Consumers have read past them. This ensures efficient memory usage while guaranteeing that no Consumer will miss messages.

Caravan drops "segments" rather than individual messages. A segment is only discarded when all active Consumers have advanced past it. The default size of a segment in Caravan is 256 entries.
