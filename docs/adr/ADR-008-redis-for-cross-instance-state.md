# ADR-008: Redis for Cross-Instance State Sharing

## Status
Accepted

## Context
The framework needs to share partition state (circuit breaker position, deduplication keys) across consumer instances during rebalance. When partition P moves from consumer A to consumer B, consumer B must know:
1. Whether the circuit breaker for P was OPEN (so it doesn't immediately resume full consumption)
2. Which events were already processed (to avoid duplicate processing)

Options considered:
- Kafka compacted topics as state store
- Redis with TTL
- PostgreSQL/JDBC
- Apache ZooKeeper

## Decision
Use Redis with TTL-based key expiry for both idempotency keys and partition state snapshots.

## Rationale

### Why Redis over Kafka compacted topics
Compacted topics require a full changelog replay on startup. With 72h retention and high throughput, that's potentially millions of keys to replay before the consumer is ready. Redis provides O(1) key lookup immediately.

### Why Redis over PostgreSQL
Deduplication requires a `contains()` check on every event — this is the hot path. Redis provides sub-millisecond latency for `EXISTS` operations. PostgreSQL would add 1-5ms per event (even with connection pooling), cutting throughput significantly.

### Why Redis over ZooKeeper
ZooKeeper is designed for coordination (leader election, distributed locks), not as a key-value store. It has a 1MB znode size limit and poor performance under high write load.

### Why TTL over manual eviction
Redis TTL provides automatic key expiry without requiring a background eviction thread. The retention period maps directly to the TTL — set once at write time, never manage again.

## Consequences

### Positive
- Sub-millisecond deduplication lookups (O(1) key existence check)
- Cross-instance state sharing without custom protocols
- TTL handles retention automatically — no eviction logic needed
- Redis Sentinel/Cluster for HA without framework changes
- `StringRedisTemplate` is well-supported in Spring ecosystem

### Negative
- Adds a runtime dependency (Redis must be available)
- Network call on every `contains()` check adds ~0.1-0.5ms per event
- Redis memory grows with event volume (mitigated by TTL)
- Single point of failure without Sentinel/Cluster (mitigated by health check + partition pause)

### Mitigation
- `IdempotencyKeyStore` interface allows fallback to `InMemoryIdempotencyKeyStore` if Redis unavailable
- Health check mechanism pauses partitions on Redis failure (same pattern as backing store unavailability)
- Redis dependency is `compileOnly` — users who don't need cross-instance dedup can skip it entirely
