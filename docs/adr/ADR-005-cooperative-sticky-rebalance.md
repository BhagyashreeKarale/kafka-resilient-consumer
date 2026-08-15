# ADR-005: Cooperative Sticky Rebalance

## Status

Accepted

## Context

Consumer group rebalances are a major source of processing disruption. With the default eager rebalance protocol, ALL partitions are revoked from ALL consumers before being reassigned — causing a full stop-the-world pause even when only one consumer joins or leaves the group.

This conflicts with the framework's core goal: healthy partitions should never stop processing due to unrelated disruptions.

Alternatives considered:
- Default eager rebalance (RangeAssignor or RoundRobinAssignor)
- Static group membership (static.group.id) to avoid rebalances entirely
- CooperativeStickyAssignor for incremental rebalances (chosen)

## Decision

Configure the consumer to use `CooperativeStickyAssignor` as the partition assignment strategy. This enables incremental cooperative rebalancing where only the partitions that need to move are revoked, while all retained partitions continue processing without interruption.

## Consequences

**Positive:**
- Only revoked partitions stop processing — retained partitions continue uninterrupted during rebalance
- Faster rebalances — no full stop-the-world, only incremental partition movement
- Less state to persist and restore — only revoked partitions need state serialization
- Sticky assignment minimizes partition movement — most partitions stay with their current consumer
- Matches the partition resilience narrative — rebalance is treated as a partition-level event, not a consumer-level event

**Negative:**
- Rebalance may take multiple rounds to reach a balanced state (cooperative protocol is incremental)
- Requires all consumers in the group to use the same assignor — cannot mix eager and cooperative
- Slightly more complex rebalance listener logic — must handle partial revocation correctly
- Not available in older Kafka client versions (requires 2.4+)
