package com.framework.resilient.circuitbreaker;

import org.apache.kafka.common.TopicPartition;

import java.time.Instant;

/**
 * Signal emitted by the health monitor when a partition's metrics exceed configured thresholds.
 *
 * @param partition    the affected Kafka partition
 * @param type         the type of degradation detected (error rate or latency)
 * @param currentValue the current metric value that triggered the signal
 * @param threshold    the configured threshold that was exceeded
 * @param timestamp    when the degradation was detected
 */
public record DegradationSignal(
        TopicPartition partition,
        DegradationType type,
        double currentValue,
        double threshold,
        Instant timestamp
) {
}
