package com.framework.resilient.circuitbreaker;

/**
 * Result of a probe batch executed during the HALF_OPEN state.
 *
 * @param totalEvents  total number of events in the probe batch
 * @param successCount number of events that processed successfully
 * @param failureCount number of events that failed processing
 * @param timedOut     whether the probe batch timeout elapsed before all events completed
 */
public record ProbeResult(
        int totalEvents,
        int successCount,
        int failureCount,
        boolean timedOut
) {
}
