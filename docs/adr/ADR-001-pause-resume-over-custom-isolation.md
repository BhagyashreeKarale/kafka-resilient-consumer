# ADR-001: Pause/Resume Over Custom Isolation

## Status

Accepted

## Context

The Kafka Resilient Consumer Framework needs to isolate unhealthy partitions without stopping the entire consumer. When a partition encounters persistent failures (poison pills, slow downstream dependencies), that partition must be taken out of the processing path while all other partitions continue uninterrupted.

Alternatives considered:
- Custom internal queue that buffers records from unhealthy partitions
- Custom routing layer that skips partitions based on health state
- Stopping and restarting the consumer with modified subscription
- Using Kafka's native `pause()`/`resume()` API

## Decision

Use Kafka's native `pause()` and `resume()` API on the `KafkaConsumer` to isolate unhealthy partitions.

When the circuit breaker transitions to OPEN for a partition, the coordinator calls `consumer.pause(Collection.of(topicPartition))`. When the cooldown expires and the circuit breaker transitions to HALF_OPEN, the coordinator calls `consumer.resume(Collection.of(topicPartition))`.

## Consequences

**Positive:**
- Zero message loss — paused partitions retain their committed offset; Kafka handles offset tracking natively
- No custom queue or routing infrastructure to build, test, or maintain
- Zero-copy isolation — records are simply not fetched, no buffering overhead
- Battle-tested mechanism used at scale by LinkedIn and Confluent
- Paused partitions do not trigger rebalance — the consumer retains assignment
- `poll()` continues to return records from non-paused partitions with no additional latency

**Negative:**
- Paused partitions still count toward `max.poll.interval.ms` — if all partitions are paused and no `poll()` returns records, the consumer must still call `poll()` within the interval to avoid being kicked from the group
- No built-in notification when a partition is paused; observability must be added at the framework level
- Partition isolation granularity is fixed at the TopicPartition level — cannot isolate a subset of keys within a partition
