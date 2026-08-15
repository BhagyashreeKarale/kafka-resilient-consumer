package com.framework.resilient.circuitbreaker;

import java.time.Duration;

/**
 * Latency percentile measurements for a partition's processing performance.
 *
 * @param p50 median (50th percentile) processing latency
 * @param p95 95th percentile processing latency
 * @param p99 99th percentile processing latency
 */
public record LatencyPercentiles(
        Duration p50,
        Duration p95,
        Duration p99
) {
}
