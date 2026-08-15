package com.framework.resilient.circuitbreaker;

import java.time.Instant;

/**
 * Health metrics snapshot for a single partition within the sliding window.
 *
 * @param errorRate           ratio of failed to total events (0.0 to 1.0)
 * @param throughputPerSecond events processed per second
 * @param latency             latency percentile measurements (p50, p95, p99)
 * @param windowStart         start of the current sliding window
 * @param totalEvents         total events observed in the current window
 */
public record PartitionHealth(
        double errorRate,
        double throughputPerSecond,
        LatencyPercentiles latency,
        Instant windowStart,
        int totalEvents
) {
}
