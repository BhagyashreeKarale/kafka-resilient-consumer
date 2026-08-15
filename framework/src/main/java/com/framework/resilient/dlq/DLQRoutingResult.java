package com.framework.resilient.dlq;

/**
 * Result of a DLQ routing attempt.
 */
public enum DLQRoutingResult {

    /** Event successfully produced to the DLQ topic. */
    ROUTED,

    /** DLQ topic unavailable — event buffered locally for retry. */
    BUFFERED,

    /** Local buffer at capacity — event rejected. */
    BUFFER_FULL
}
