package com.framework.resilient.circuitbreaker;

import java.time.Duration;
import java.time.Instant;

/**
 * Immutable record representing the internal state of a single partition's circuit breaker.
 * State transitions produce new instances rather than mutating existing ones.
 *
 * @param state                current circuit breaker state
 * @param stateEnteredAt       when the current state was entered
 * @param lastTransitionTime   when the last state transition occurred
 * @param consecutiveFailures  number of consecutive probe failures
 * @param probeEventsProcessed number of events processed during current HALF_OPEN probe
 * @param probeEventsSucceeded number of successful events during current HALF_OPEN probe
 */
public record CircuitBreakerState(
        CircuitState state,
        Instant stateEnteredAt,
        Instant lastTransitionTime,
        int consecutiveFailures,
        int probeEventsProcessed,
        int probeEventsSucceeded
) {

    /**
     * Creates the initial state for a newly assigned partition (CLOSED).
     */
    public static CircuitBreakerState initial() {
        Instant now = Instant.now();
        return new CircuitBreakerState(CircuitState.CLOSED, now, now, 0, 0, 0);
    }

    /**
     * Checks whether the cooldown period has expired while in OPEN state.
     *
     * @param cooldownPeriod the configured cooldown duration
     * @return true if state is OPEN and cooldown has elapsed
     */
    public boolean isCooldownExpired(Duration cooldownPeriod) {
        return state == CircuitState.OPEN
                && Instant.now().isAfter(stateEnteredAt.plus(cooldownPeriod));
    }

    /**
     * Creates a new state representing the transition to the specified target state.
     * Resets probe counters when entering HALF_OPEN, resets failure count when entering CLOSED.
     *
     * @param newState the target circuit breaker state
     * @return a new CircuitBreakerState reflecting the transition
     */
    public CircuitBreakerState transitionTo(CircuitState newState) {
        Instant now = Instant.now();
        return new CircuitBreakerState(
                newState,
                now,
                now,
                newState == CircuitState.CLOSED ? 0 : consecutiveFailures + (newState == CircuitState.OPEN ? 1 : 0),
                newState == CircuitState.HALF_OPEN ? 0 : probeEventsProcessed,
                newState == CircuitState.HALF_OPEN ? 0 : probeEventsSucceeded
        );
    }
}
