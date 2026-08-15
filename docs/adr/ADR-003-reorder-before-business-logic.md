# ADR-003: Reorder Before Business Logic

## Status

Accepted

## Context

Partition isolation, retries, and redeliveries create apparent disorder in the event stream. When a partition is paused and later resumed, or when events are retried after transient failures, the business handler may receive events out of their original sequence order relative to a source entity.

Alternatives considered:
- Let the business handler deal with out-of-order events (push complexity downstream)
- Reorder after business logic (too late — handler already made decisions on stale order)
- Buffer and reorder events before delivering to the business handler (chosen)

## Decision

Place the `ReorderBuffer` in the processing pipeline BEFORE events reach the business handler. The buffer tracks expected sequence numbers per source entity within each partition and holds out-of-order events until predecessors arrive or a configurable gap timeout fires.

The pipeline order is: Deserialize → Deduplication → **Reorder** → Business Logic → Commit.

## Consequences

**Positive:**
- Simpler handler contracts — business logic always sees an ordered stream per source entity
- Correctness by default — handlers don't need defensive out-of-order handling
- Gap timeout (default 5 seconds) prevents unbounded buffering when predecessors are permanently lost
- Backpressure integration — buffer can pause the partition if it fills up, preventing OOM
- Framework responsibility — ordering guarantee is a framework concern, not an application concern

**Negative:**
- Adds latency for out-of-order events (up to the gap timeout duration)
- Memory consumption grows with disorder — more gaps mean more buffered events
- Sequence number convention must be agreed upon between producers and consumers
- Gap timeout releases may produce a warning log but the handler must still process the event
