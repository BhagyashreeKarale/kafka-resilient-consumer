package com.framework.resilient.circuitbreaker;

/**
 * Represents the state of a per-partition circuit breaker.
 */
public enum CircuitState {

    /** Normal processing — partition is actively consumed. */
    CLOSED,

    /** Partition paused — cooldown timer running, waiting to probe. */
    OPEN,

    /** Probe batch in progress — consuming limited events to test recovery. */
    HALF_OPEN
}
