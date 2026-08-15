package com.framework.resilient.reorder;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the reorder buffer.
 *
 * @param maxBufferSize    maximum events per partition buffer (default 10000, range 100-1000000)
 * @param resumeThreshold  occupancy ratio at which to resume a paused partition (default 0.8)
 * @param reorderTimeout   maximum wait time for out-of-order events (default 5s, range 100ms-60s)
 */
@ConfigurationProperties(prefix = "resilient.consumer.reorder-buffer")
public record ReorderBufferProperties(
        int maxBufferSize,
        double resumeThreshold,
        Duration reorderTimeout
) {

    /**
     * Compact constructor that validates all configuration ranges.
     */
    public ReorderBufferProperties {
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException(
                    "maxBufferSize must be > 0, got: " + maxBufferSize);
        }
        if (resumeThreshold < 0.0 || resumeThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "resumeThreshold must be between 0.0 and 1.0, got: " + resumeThreshold);
        }
        if (reorderTimeout == null || reorderTimeout.isNegative() || reorderTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "reorderTimeout must be positive, got: " + reorderTimeout);
        }
    }

    /**
     * Creates properties with default values.
     */
    public ReorderBufferProperties() {
        this(
                10_000,
                0.8,
                Duration.ofSeconds(5)
        );
    }
}
