# ADR-007: Single-Threaded Poll Loop Over Thread-Per-Partition

## Status
Accepted

## Context
When designing the consumer coordinator, we needed to decide how to structure the consumption and processing threads. The primary options were:
1. Single thread owning the KafkaConsumer, processing records sequentially in the poll loop
2. Thread-per-partition: poll loop dispatches records to per-partition worker threads
3. Thread pool: poll loop dispatches records to a shared thread pool

## Decision
We use a single-threaded poll loop where one thread owns the KafkaConsumer instance and processes all records sequentially.

## Rationale

### KafkaConsumer is NOT thread-safe
The Kafka documentation is explicit: "The Kafka consumer is NOT thread-safe." Calling any method from a different thread (except wakeup()) results in a ConcurrentModificationException. This eliminates naive multi-threaded approaches without synchronization.

### Pause/resume semantics require poll-loop ownership
Our circuit breaker calls `consumer.pause()` and `consumer.resume()` based on partition health. These calls must happen from the same thread that calls `poll()`. Multi-threaded designs require message-passing back to the poll thread to invoke pause/resume, adding complexity and latency.

### Ordering guarantees within a partition
Kafka guarantees ordering within a partition. If we dispatch records to a thread pool, we lose this guarantee unless we add per-partition queues with head-of-line blocking — essentially reinventing a single-threaded model per partition with extra overhead.

### Scalability via consumer group members
Kafka's native scaling model is horizontal: add more consumer group members, each running its own poll loop. This matches our design perfectly — each coordinator instance handles a subset of partitions single-threaded, and the consumer group protocol distributes load.

### Simplicity
A single-threaded poll loop eliminates an entire class of concurrency bugs: no need for locks on offset tracking, no race conditions between processing and commits, no complex shutdown coordination across thread pools.

## Consequences

### Positive
- Zero concurrency overhead on the hot path
- ConcurrentHashMap for partition state (safe reads from metrics/health threads)
- No lock contention between partitions — each poll processes records sequentially
- Simple reasoning about offset safety: commit after processing, never before
- Predictable memory usage — no unbounded work queues

### Negative
- Throughput per coordinator instance is bounded by single-thread processing speed
- Long-running handlers block all partitions (mitigated by circuit breaker and bounded retries)
- Retry backoff blocks the poll loop (mitigated by short default backoffs)

### Mitigation
- Scale horizontally via consumer group members (the Kafka way)
- Keep handler execution fast (< 100ms target)
- Use bounded retries with short backoffs (default: 1s, 2s, 4s = 7s max)
- Circuit breaker isolates slow partitions before they accumulate too much blocking

## Alternatives Considered

### Thread-per-partition
Rejected because: requires message-passing for pause/resume, loses ordering guarantees without per-partition synchronization, and adds significant complexity for marginal throughput gain in most workloads.

### Virtual threads (Java 21)
Considered but deferred: virtual threads could eliminate the retry-blocking issue by yielding during sleep. However, KafkaConsumer is still not thread-safe, so the poll loop itself must remain single-threaded. Virtual threads are a candidate for future retry-queue optimization.

### Reactive (Project Reactor / Vert.x)
Rejected because: abstracts away the Kafka consumer lifecycle control we need (direct pause/resume), introduces framework coupling, and makes debugging significantly harder for a library meant to be embedded.
