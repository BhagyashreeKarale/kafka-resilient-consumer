package com.framework.resilient.circuitbreaker;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the per-partition circuit breaker.
 *
 * @param errorRateThreshold     error rate to trigger OPEN state (default 0.5, range 0.01-1.0)
 * @param latencyThreshold       p99 latency to trigger OPEN state (default 5000ms, range 100-60000ms)
 * @param cooldownPeriod         duration in OPEN before transitioning to HALF_OPEN (default 30s, range 5-300s)
 * @param probeBatchSize         number of events to consume during HALF_OPEN probe (default 10)
 * @param probeBatchTimeout      maximum wait time for probe batch completion (default 30s)
 * @param detectionLatencyTarget target time from threshold breach to signal emission (default 2s)
 * @param reactionLatencyTarget  target time from signal to pause() invocation (default 1s)
 */
@ConfigurationProperties(prefix = "resilient.consumer.circuit-breaker")
public record CircuitBreakerProperties(
        double errorRateThreshold,
        Duration latencyThreshold,
        Duration cooldownPeriod,
        int probeBatchSize,
        Duration probeBatchTimeout,
        Duration detectionLatencyTarget,
        Duration reactionLatencyTarget
) {

    /**
     * Compact constructor that validates all configuration ranges.
     */
    public CircuitBreakerProperties {
        if (errorRateThreshold < 0.01 || errorRateThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "errorRateThreshold must be between 0.01 and 1.0, got: " + errorRateThreshold);
        }
        if (latencyThreshold == null || latencyThreshold.isNegative() || latencyThreshold.isZero()) {
            throw new IllegalArgumentException(
                    "latencyThreshold must be positive, got: " + latencyThreshold);
        }
        if (cooldownPeriod == null || cooldownPeriod.isNegative() || cooldownPeriod.isZero()) {
            throw new IllegalArgumentException(
                    "cooldownPeriod must be positive, got: " + cooldownPeriod);
        }
        if (probeBatchSize <= 0) {
            throw new IllegalArgumentException("probeBatchSize must be > 0, got: " + probeBatchSize);
        }
        if (probeBatchTimeout == null || probeBatchTimeout.isNegative() || probeBatchTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "probeBatchTimeout must be positive, got: " + probeBatchTimeout);
        }
        if (detectionLatencyTarget == null || detectionLatencyTarget.isNegative() || detectionLatencyTarget.isZero()) {
            throw new IllegalArgumentException(
                    "detectionLatencyTarget must be positive, got: " + detectionLatencyTarget);
        }
        if (reactionLatencyTarget == null || reactionLatencyTarget.isNegative() || reactionLatencyTarget.isZero()) {
            throw new IllegalArgumentException(
                    "reactionLatencyTarget must be positive, got: " + reactionLatencyTarget);
        }
    }

    /**
     * Creates properties with default values.
     */
    public CircuitBreakerProperties() {
        this(
                0.5,
                Duration.ofMillis(5000),
                Duration.ofSeconds(30),
                10,
                Duration.ofSeconds(30),
                Duration.ofSeconds(2),
                Duration.ofSeconds(1)
        );
    }
}
