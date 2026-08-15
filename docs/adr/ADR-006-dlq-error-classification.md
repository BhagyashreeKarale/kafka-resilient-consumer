# ADR-006: DLQ Error Classification

## Status

Accepted

## Context

Failed events need somewhere to go. Without a Dead Letter Queue, poison pills block partition progress indefinitely or are silently dropped. However, not all failures are equal — a deserialization error (bad data from producer) requires a completely different investigation and remediation path than a transient timeout (downstream system temporarily unavailable).

Alternatives considered:
- Single DLQ topic for all failures
- Discard failures after retry exhaustion (data loss)
- Classify errors into categories with separate DLQ topics per classification (chosen)

## Decision

Classify processing errors into four categories and route each to a dedicated DLQ topic:

1. **DESERIALIZATION** — event cannot be parsed (schema mismatch, corrupt payload)
2. **VALIDATION** — event parsed but fails business validation rules
3. **TRANSIENT** — retryable failure that exhausted retry budget (timeouts, temporary unavailability)
4. **PERMANENT** — non-retryable business logic failure (invalid state transitions, constraint violations)

A catch-all default topic receives events with unmapped classifications to prevent silent data loss.

## Consequences

**Positive:**
- Independent investigation per failure type — teams can subscribe to relevant DLQ topics
- Different reprocessing strategies per category — transient errors may be replayed automatically after recovery, deserialization errors need producer fixes
- Deserialization errors skip the full pipeline (no dedup, no reorder) — routed directly to DLQ
- Catch-all topic prevents silent data loss for unexpected error types
- Header enrichment on DLQ events provides full provenance (original topic, partition, offset, timestamp, retry count)
- Enables targeted alerting — alert on permanent errors immediately, batch-alert on transient errors

**Negative:**
- More topics to manage — four DLQ topics plus catch-all per consumer group
- Classification logic must be maintained as new error types emerge
- Misclassification routes events to wrong topic — requires careful exception hierarchy design
- DLQ topics need their own retention and monitoring policies
