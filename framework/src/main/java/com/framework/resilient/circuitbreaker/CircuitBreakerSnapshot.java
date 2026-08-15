package com.framework.resilient.circuitbreaker;

import java.time.Instant;

/**
 * Snapshot of a partition's circuit breaker state for persistence during rebalance.
 *
 * @param state               current circuit breaker state
 * @param stateEnteredAt      when the current state was entered
 * @param consecutiveFailures number of consecutive failures recorded
 * @param lastTransitionTime  when the last state transition occurred
 */
public record CircuitBreakerSnapshot(
        CircuitState state,
        Instant stateEnteredAt,
        int consecutiveFailures,
        Instant lastTransitionTime
) {
}
