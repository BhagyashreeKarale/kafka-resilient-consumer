package com.framework.resilient.coordinator;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the Consumer Coordinator.
 *
 * @param pollTimeout              poll loop timeout (default 100ms)
 * @param maxPollRecords           max records per poll (default 500)
 * @param shutdownTimeout          graceful shutdown timeout (default 30s, range 5-120s)
 * @param rebalanceCommitTimeout   offset commit timeout during rebalance (default 5s)
 * @param statePersistenceTimeout  state persistence timeout during rebalance (default 5s)
 * @param stateRestorationTimeout  state restoration timeout during rebalance (default 10s)
 * @param startupRestorationTimeout state restoration timeout at startup (default 30s)
 */
@ConfigurationProperties(prefix = "resilient.consumer")
public record CoordinatorProperties(
        Duration pollTimeout,
        int maxPollRecords,
        Duration shutdownTimeout,
        Duration rebalanceCommitTimeout,
        Duration statePersistenceTimeout,
        Duration stateRestorationTimeout,
        Duration startupRestorationTimeout
) {

    /**
     * Compact constructor that validates all configuration ranges.
     */
    public CoordinatorProperties {
        if (pollTimeout == null || pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive, got: " + pollTimeout);
        }
        if (maxPollRecords <= 0) {
            throw new IllegalArgumentException("maxPollRecords must be > 0, got: " + maxPollRecords);
        }
        if (shutdownTimeout == null || shutdownTimeout.isNegative() || shutdownTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "shutdownTimeout must be positive, got: " + shutdownTimeout);
        }
        if (rebalanceCommitTimeout == null || rebalanceCommitTimeout.isNegative() || rebalanceCommitTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "rebalanceCommitTimeout must be positive, got: " + rebalanceCommitTimeout);
        }
        if (statePersistenceTimeout == null || statePersistenceTimeout.isNegative() || statePersistenceTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "statePersistenceTimeout must be positive, got: " + statePersistenceTimeout);
        }
        if (stateRestorationTimeout == null || stateRestorationTimeout.isNegative() || stateRestorationTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "stateRestorationTimeout must be positive, got: " + stateRestorationTimeout);
        }
        if (startupRestorationTimeout == null || startupRestorationTimeout.isNegative() || startupRestorationTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "startupRestorationTimeout must be positive, got: " + startupRestorationTimeout);
        }
    }

    /**
     * Creates properties with default values.
     */
    public CoordinatorProperties() {
        this(
                Duration.ofMillis(100),
                500,
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofSeconds(10),
                Duration.ofSeconds(30)
        );
    }
}
