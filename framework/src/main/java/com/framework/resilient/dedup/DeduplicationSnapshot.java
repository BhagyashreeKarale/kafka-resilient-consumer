package com.framework.resilient.dedup;

import java.time.Instant;
import java.util.Map;

/**
 * Snapshot of deduplication state for a partition, used for persistence during rebalance.
 *
 * @param keys map of idempotency key to the timestamp when it was processed
 */
public record DeduplicationSnapshot(
        Map<String, Instant> keys
) {
}
