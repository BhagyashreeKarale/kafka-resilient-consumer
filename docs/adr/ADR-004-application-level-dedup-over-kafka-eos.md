# ADR-004: Application-Level Deduplication Over Kafka EOS

## Status

Accepted

## Context

Partition isolation and retries create duplicate events. When a partition is paused and resumed, or when processing fails and events are redelivered from the last committed offset, the same event may arrive at the business handler more than once. The framework needs exactly-once processing semantics.

Kafka provides Exactly-Once Semantics (EOS) via idempotent producers and transactions. However, EOS requires that ALL downstream writes participate in Kafka transactions — including database writes, API calls, and other side effects.

Alternatives considered:
- Kafka Exactly-Once Semantics (idempotent producer + transactions)
- Application-level idempotency key tracking (chosen)

## Decision

Implement application-level deduplication using idempotency key tracking within the framework. The `DeduplicationEngine` computes a key from `(sourceEntity, sequenceNumber, partition)` and checks it against a pluggable `IdempotencyKeyStore` before allowing processing. Keys are retained for a configurable window (default 72 hours).

## Consequences

**Positive:**
- Works across any downstream system — does not require downstream to participate in Kafka transactions
- Simpler operational model — no transaction coordinators, no two-phase commit
- Lower latency — no transaction barrier on every batch
- EOS is overkill when only consumer-side dedup is needed (we don't produce back to Kafka as part of processing)
- Pluggable backing store — in-memory for testing, external store for production
- Configurable retention window trades memory for dedup coverage

**Negative:**
- Dedup window is bounded — events replayed after the retention period may be processed again
- Requires a reliable backing store for the idempotency key set in production
- Backing store unavailability pauses affected partitions (safety over availability trade-off)
- Does not protect against duplicates introduced by the producer — only consumer-side redeliveries
