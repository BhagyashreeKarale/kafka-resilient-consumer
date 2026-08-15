package com.framework.resilient.autoconfigure;

import com.framework.resilient.circuitbreaker.CircuitState;
import com.framework.resilient.metrics.HealthStatus;
import com.framework.resilient.metrics.MetricsExporter;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring Boot Actuator health indicator for the Resilient Consumer Framework.
 *
 * <p>Reports health based on partition circuit breaker states:
 * <ul>
 *   <li><strong>UP</strong>: No partitions in OPEN state — all partitions healthy</li>
 *   <li><strong>DOWN</strong>: One or more partitions in OPEN state — degraded processing</li>
 * </ul>
 *
 * <p>Health details include consumer group status, partition count,
 * circuit breaker state summary, and a list of any OPEN (degraded) partitions.
 */
@Component
public class ResilientConsumerHealthIndicator implements HealthIndicator {

    private final MetricsExporter metricsExporter;

    public ResilientConsumerHealthIndicator(MetricsExporter metricsExporter) {
        this.metricsExporter = metricsExporter;
    }

    @Override
    public Health health() {
        HealthStatus status = metricsExporter.getHealthStatus();

        // Count circuit breaker states
        Map<CircuitState, Integer> stateSummary = new HashMap<>();
        for (CircuitState state : CircuitState.values()) {
            stateSummary.put(state, 0);
        }

        List<String> openPartitions = new ArrayList<>();

        for (Map.Entry<TopicPartition, CircuitState> entry : status.circuitBreakerStates().entrySet()) {
            CircuitState state = entry.getValue();
            stateSummary.merge(state, 1, Integer::sum);

            if (state == CircuitState.OPEN) {
                openPartitions.add(entry.getKey().topic() + "-" + entry.getKey().partition());
            }
        }

        // Build health details
        Health.Builder builder;
        if (openPartitions.isEmpty()) {
            builder = Health.up();
        } else {
            builder = Health.down();
            builder.withDetail("degradedPartitions", openPartitions);
        }

        builder.withDetail("consumerStatus", status.consumerGroupStatus());
        builder.withDetail("partitionCount", status.assignedPartitions().size());
        builder.withDetail("circuitBreakerStates", stateSummary);

        return builder.build();
    }
}
