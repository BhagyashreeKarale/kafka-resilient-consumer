package com.framework.resilient.metrics;

import com.framework.resilient.circuitbreaker.CircuitState;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * Health status data for the health check endpoint.
 *
 * @param consumerGroupStatus   current consumer group status (e.g., "STABLE", "REBALANCING")
 * @param assignedPartitions    set of currently assigned partitions
 * @param circuitBreakerStates  map of partition to current circuit breaker state
 * @param reorderBufferDepths   map of partition to current buffer depth
 */
public record HealthStatus(
        String consumerGroupStatus,
        Set<TopicPartition> assignedPartitions,
        Map<TopicPartition, CircuitState> circuitBreakerStates,
        Map<TopicPartition, Integer> reorderBufferDepths
) {
}
