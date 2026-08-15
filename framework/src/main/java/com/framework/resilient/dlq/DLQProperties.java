package com.framework.resilient.dlq;

import java.time.Duration;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the Dead Letter Queue router.
 *
 * @param maxRetryCount        max processing retries before DLQ routing (default 3, range 1-10)
 * @param initialBackoff       initial backoff duration for retries (default 1s)
 * @param maxBackoff           maximum backoff cap for retries (default 60s)
 * @param maxBufferSize        max locally buffered DLQ events when DLQ unavailable (default 1000)
 * @param maxDlqRetryAttempts  max retry attempts for DLQ production (default 10)
 * @param topicMappings        mapping from error classification to DLQ topic name
 * @param defaultTopic         catch-all DLQ topic for unmapped classifications
 */
@ConfigurationProperties(prefix = "resilient.consumer.dlq")
public record DLQProperties(
        int maxRetryCount,
        Duration initialBackoff,
        Duration maxBackoff,
        int maxBufferSize,
        int maxDlqRetryAttempts,
        Map<ErrorClassification, String> topicMappings,
        String defaultTopic
) {

    /**
     * Compact constructor that validates all configuration ranges.
     */
    public DLQProperties {
        if (maxRetryCount < 1 || maxRetryCount > 10) {
            throw new IllegalArgumentException(
                    "maxRetryCount must be between 1 and 10, got: " + maxRetryCount);
        }
        if (initialBackoff == null || initialBackoff.isNegative()) {
            throw new IllegalArgumentException(
                    "initialBackoff must not be negative, got: " + initialBackoff);
        }
        if (maxBackoff == null || maxBackoff.isNegative()) {
            throw new IllegalArgumentException(
                    "maxBackoff must not be negative, got: " + maxBackoff);
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException(
                    "maxBufferSize must be > 0, got: " + maxBufferSize);
        }
        if (maxDlqRetryAttempts <= 0) {
            throw new IllegalArgumentException(
                    "maxDlqRetryAttempts must be > 0, got: " + maxDlqRetryAttempts);
        }
    }

    /**
     * Creates properties with default values.
     */
    public DLQProperties() {
        this(
                3,
                Duration.ofSeconds(1),
                Duration.ofSeconds(60),
                1000,
                10,
                Map.of(),
                "dlq.unclassified"
        );
    }
}
