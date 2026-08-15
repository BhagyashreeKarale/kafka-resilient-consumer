package com.framework.resilient.dedup;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the deduplication engine.
 *
 * @param retentionPeriod      how long to retain idempotency keys (default 72h, range 24-168h)
 * @param evictionInterval     interval between expired key eviction runs (default 1h)
 * @param healthCheckInterval  interval between backing store health checks (default 5s, range 1-30s)
 */
@ConfigurationProperties(prefix = "resilient.consumer.deduplication")
public record DeduplicationProperties(
        Duration retentionPeriod,
        Duration evictionInterval,
        Duration healthCheckInterval
) {

    /**
     * Compact constructor that validates all configuration ranges.
     */
    public DeduplicationProperties {
        if (retentionPeriod == null || retentionPeriod.isNegative() || retentionPeriod.isZero()) {
            throw new IllegalArgumentException(
                    "retentionPeriod must be positive, got: " + retentionPeriod);
        }
        if (evictionInterval == null || evictionInterval.isNegative() || evictionInterval.isZero()) {
            throw new IllegalArgumentException(
                    "evictionInterval must be positive, got: " + evictionInterval);
        }
        if (healthCheckInterval == null || healthCheckInterval.isNegative() || healthCheckInterval.isZero()) {
            throw new IllegalArgumentException(
                    "healthCheckInterval must be positive, got: " + healthCheckInterval);
        }
    }

    /**
     * Creates properties with default values.
     */
    public DeduplicationProperties() {
        this(
                Duration.ofHours(72),
                Duration.ofHours(1),
                Duration.ofSeconds(5)
        );
    }
}
