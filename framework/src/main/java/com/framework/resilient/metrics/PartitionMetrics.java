package com.framework.resilient.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;

/**
 * Holds Micrometer meter references for a single partition.
 *
 * @param circuitBreakerState   gauge representing the current circuit breaker state ordinal
 * @param processedEvents       counter for successfully processed events
 * @param failedEvents          counter for failed events
 * @param processingLatency     timer for measuring processing latency with percentile histograms
 * @param reorderBufferDepth    gauge for current reorder buffer depth
 * @param deduplicationCacheSize gauge for current deduplication key store size
 */
public record PartitionMetrics(
        Gauge circuitBreakerState,
        Counter processedEvents,
        Counter failedEvents,
        Timer processingLatency,
        Gauge reorderBufferDepth,
        Gauge deduplicationCacheSize
) {
}
