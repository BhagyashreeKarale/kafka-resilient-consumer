# ADR-002: Per-Partition Circuit Breakers

## Status

Accepted

## Context

The framework needs to isolate failures at the correct granularity. Kafka consumers can be assigned multiple partitions, and failures are often localized — a poison pill in partition 3, a slow downstream shard backing partition 7, or corrupt data from a specific producer writing to partition 1.

A single consumer-level circuit breaker would pause ALL partitions when any one partition degrades, unnecessarily halting healthy work.

Alternatives considered:
- One circuit breaker per consumer instance
- One circuit breaker per topic
- One circuit breaker per TopicPartition (chosen)

## Decision

Maintain one independent circuit breaker state machine per TopicPartition. Each circuit breaker tracks its own state (CLOSED, OPEN, HALF_OPEN), cooldown timer, and probe results without affecting any other partition's breaker.

The `PartitionCircuitBreaker` component uses a `ConcurrentHashMap<TopicPartition, CircuitBreakerState>` to store per-partition state, and state transitions on one partition have zero side effects on others.

## Consequences

**Positive:**
- Partition is Kafka's unit of parallelism — circuit breaker granularity matches the data model
- Failures stay contained: a bad partition never degrades healthy ones
- Independent cooldown and recovery per partition — fast recovery where the issue resolves first
- Clean lifecycle: breakers are initialized on partition assignment and removed on revocation
- Enables precise observability — metrics per partition show exactly which partitions are unhealthy

**Negative:**
- Memory overhead scales with partition count (one state machine per assigned partition)
- State persistence on rebalance must serialize/restore per-partition breaker states
- Correlated failures across partitions (e.g., downstream system fully down) will open multiple breakers independently, which is correct but may generate many alerts
