# Kafka Resilient Consumer Framework

**Problem**: In a standard Kafka consumer, a single "poison pill" message or a slow downstream dependency on one partition stalls processing for ALL partitions on that instance — because they share one poll loop. In production at scale, this means one bad tenant or one database shard going slow cascades into full consumer lag across the entire consumer group.

**Solution**: This framework introduces **partition-level failure isolation** using Kafka's native `pause()`/`resume()` API. Each partition gets its own circuit breaker, health monitor, reorder buffer, and deduplication state. When one partition degrades, only that partition pauses — the other partitions continue processing normally.

---

## Why This Exists

I built this after observing a recurring pattern in event-driven systems: Kafka consumers that either fail completely or succeed completely, with nothing in between. The Kafka client API gives you `pause()` and `resume()` at the partition level, but almost nobody uses them for intelligent failure isolation. Instead, teams build thread-per-partition architectures (complex), use Kafka Streams (heavyweight for simple consumers), or just let the whole consumer restart (losing progress).

This framework is the middle ground: **a single-threaded poll loop with per-partition intelligence**.

## Processing Pipeline

```
Record from poll()
  → Deserialize (fail? → DLQ immediately)
  → Circuit breaker check (partition OPEN? → skip, wait for probe)
  → Reorder buffer (out-of-sequence? → buffer until sequence gap fills)
  → Deduplication (already processed? → skip, commit offset)
  → Business logic handler (transient fail? → retry with backoff → DLQ)
  → Record offset for batched commit
```

Each record goes through this pipeline on the single `resilient-consumer-poll` thread. No concurrent writes, no lock contention, no visibility issues.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Consumer Coordinator                          │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Health     │  │   Circuit    │  │   Reorder Buffer     │  │
│  │   Monitor    │──│   Breaker    │  │   (per partition)    │  │
│  │   (sliding   │  │   (per       │  │                      │  │
│  │    window)   │  │   partition) │  │   Sequence tracking  │  │
│  └──────────────┘  └──────────────┘  │   Gap detection      │  │
│                                       │   Timeout flush      │  │
│  ┌──────────────┐  ┌──────────────┐  └──────────────────────┘  │
│  │   Dedup      │  │   DLQ        │                             │
│  │   Engine     │  │   Router     │  ┌──────────────────────┐  │
│  │   (InMemory  │  │   (error     │  │   Metrics Exporter   │  │
│  │    or Redis) │  │   classified)│  │   (per-partition     │  │
│  └──────────────┘  └──────────────┘  │    Prometheus)       │  │
│                                       └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Circuit Breaker State Machine

```
CLOSED ──── error rate > threshold OR p99 latency > threshold ────► OPEN
  ▲                                                                   │
  │                                                                   │
  │ probe batch succeeds                              cooldown elapses│
  │                                                                   │
  └──────────── HALF_OPEN ◄───────────────────────────────────────────┘
                    │
                    │ probe batch fails
                    └────────────────────────────────────────────► OPEN
```

When a partition enters OPEN state, the coordinator calls `consumer.pause(partition)`. Kafka's poll loop continues returning records for other partitions. When cooldown elapses, the coordinator calls `consumer.resume(partition)` and processes a small probe batch. If the probe succeeds, the partition returns to CLOSED.

## Module Structure

```
kafka-resilient-consumer/
├── framework/      # The library — zero domain knowledge, fully reusable
├── demo/           # Payment-service exercising the framework end-to-end
├── benchmarks/     # Performance harness (Testcontainers + JUnit 5)
└── docs/adr/       # 8 Architecture Decision Records
```

## Quick Start

**Prerequisites**: Java 21, Docker Desktop

```bash
# 1. Build (skip tests — they need Docker)
./gradlew build -x test

# 2. Start Kafka (KRaft mode, no Zookeeper)
cd demo && docker compose up -d

# 3. Wait for Kafka to become healthy (~30 seconds)
docker compose ps  # should show "healthy"

# 4. Create topics (if kafka-init didn't run)
docker exec demo-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic payment-events --partitions 6 --replication-factor 1

# 5. Run the demo app
cd .. && ./gradlew :demo:bootRun

# 6. Produce events
curl -X POST "http://localhost:8080/produce/sequenced?accountId=acc-001&count=100"
curl -X POST "http://localhost:8080/produce/out-of-order?accountId=acc-002&count=50"
curl -X POST "http://localhost:8080/produce/duplicates?accountId=acc-001&seq=5"

# 7. Check metrics
curl http://localhost:8080/actuator/prometheus | grep resilient_consumer

# 8. Cleanup
cd demo && docker compose down
```

## Key Design Decisions

| Decision | Why |
|----------|-----|
| `pause()`/`resume()` over thread-per-partition | Works with cooperative-sticky rebalance, no rebalance storms, simpler concurrency model |
| Single-threaded poll loop | No locks, no visibility issues, scales horizontally via consumer instances |
| Application-level dedup over Kafka EOS | EOS has ~30% throughput overhead and doesn't survive consumer group resets |
| Reorder before business logic | Business handlers shouldn't need to handle out-of-order delivery |
| Cooperative sticky rebalance | Minimizes partition migration, preserves circuit breaker state |
| Redis for cross-instance state | During rebalance, new partition owner can restore CB state without cold start |
| DLQ error classification | Different error types need different retention, alerting, and replay strategies |
| Fixed sliding window (not EWMA) | Simpler, O(1) bucket operations, predictable memory |

Full reasoning in [`docs/adr/`](docs/adr/).

## Extension Points

```java
// Your business logic — implement this interface
public interface EventHandler<T> {
    ProcessingResult handle(T payload, EventMetadata metadata);
}

// Your deserialization — convert bytes to domain events
public interface EventDeserializer<T> {
    SequencedEvent<T> deserialize(ConsumerRecord<String, byte[]> record);
}

// Swap dedup backend (default: in-memory; Redis included)
public interface IdempotencyKeyStore { ... }

// Swap state persistence (default: file-based JSON; Redis included)
public interface StateStore { ... }
```

## Configuration

All properties under `resilient.consumer.*` with validation at startup:

```yaml
resilient:
  consumer:
    circuit-breaker:
      error-rate-threshold: 0.5       # 0.01–1.0
      latency-threshold: 5000ms       # p99 threshold
      cooldown-period: 30s            # OPEN → HALF_OPEN wait
      probe-batch-size: 10            # events in probe
    health-monitor:
      sliding-window-size: 60s        # metric aggregation window
      evaluation-interval: 1s         # how often to check thresholds
    reorder-buffer:
      max-buffer-size: 10000          # per-partition cap
      reorder-timeout: 5s             # max wait for gap fill
    deduplication:
      retention-period: 72h           # key TTL
    dlq:
      max-retry-count: 3              # retries before DLQ
      initial-backoff: 500ms          # exponential backoff start
```

## Technology Stack

| Tech | Version | Why |
|------|---------|-----|
| Java | 21 | Records, sealed interfaces, pattern matching, virtual threads (future) |
| Spring Boot | 3.2.5 | Auto-configuration, actuator, property binding |
| Kafka Client | 3.6.2 | KRaft support, cooperative rebalance protocol |
| Micrometer | 1.12.5 | Per-partition Prometheus metrics |
| Testcontainers | 1.19.8 | Isolated integration tests with real Kafka |
| Gradle | 8.14.2 | Multi-module build, toolchain management |

## Tests

```bash
# Unit tests (no Docker needed) — 172 tests
./gradlew :framework:test --tests "com.framework.resilient.circuitbreaker.*" \
                          --tests "com.framework.resilient.coordinator.*" \
                          --tests "com.framework.resilient.dedup.*" \
                          --tests "com.framework.resilient.dlq.*" \
                          --tests "com.framework.resilient.metrics.*" \
                          --tests "com.framework.resilient.reorder.*"

# Integration tests (Docker required)
./gradlew :framework:test --tests "com.framework.resilient.integration.*"

# Benchmarks (Docker required, ~5 minutes)
./gradlew :benchmarks:test
```

## What This Demonstrates

For an interviewer evaluating this project:

1. **Kafka internals knowledge** — `pause()`/`resume()`, cooperative-sticky assignment, offset management, consumer group protocol
2. **Distributed systems patterns** — circuit breakers, sliding windows, exactly-once semantics, state machine design
3. **Production thinking** — graceful shutdown, state persistence across rebalances, backpressure, structured logging with correlation IDs
4. **Software engineering** — interfaces for extension, Spring Boot starter auto-configuration, multi-module build, ADRs explaining every trade-off
5. **Testing discipline** — unit tests with mocks, integration tests with Testcontainers, benchmark harness for performance claims

## Future Work (deliberately deferred)

- EWMA load tracking for more responsive degradation detection
- Circuit breaker → half-open probe with synthetic health-check events
- Redis-backed sliding window for cross-instance metric aggregation
- Configurable retry policies per error classification
- gRPC health reporting for orchestrator integration
- Adaptive backoff based on downstream recovery signals

## License

MIT
