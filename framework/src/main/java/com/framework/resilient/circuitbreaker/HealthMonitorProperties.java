package com.framework.resilient.circuitbreaker;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the partition health monitor.
 *
 * @param slidingWindowSize   size of the sliding time window for metrics (default 60s, range 10-300s)
 * @param evaluationInterval  interval between health evaluations (default 1s)
 */
@ConfigurationProperties(prefix = "resilient.consumer.health-monitor")
public record HealthMonitorProperties(
        Duration slidingWindowSize,
        Duration evaluationInterval
) {

    /**
     * Creates properties with default values.
     */
    public HealthMonitorProperties() {
        this(
                Duration.ofSeconds(60),
                Duration.ofSeconds(1)
        );
    }
}
