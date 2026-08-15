package com.framework.resilient.metrics;

import com.framework.resilient.circuitbreaker.CircuitState;
import com.framework.resilient.dlq.ErrorClassification;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsExporterTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);

    private SimpleMeterRegistry registry;
    private MetricsExporter exporter;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        exporter = new MetricsExporter(registry);
    }

    // --- registerPartition creates all expected meters ---

    @Test
    void registerPartition_shouldCreateCircuitBreakerStateGauge() {
        exporter.registerPartition(PARTITION_0);

        Gauge gauge = registry.find("resilient.consumer.circuit.breaker.state")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(CircuitState.CLOSED.ordinal());
    }

    @Test
    void registerPartition_shouldCreateProcessedEventsCounter() {
        exporter.registerPartition(PARTITION_0);

        Counter counter = registry.find("resilient.consumer.events.processed")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isZero();
    }

    @Test
    void registerPartition_shouldCreateFailedEventsCounter() {
        exporter.registerPartition(PARTITION_0);

        Counter counter = registry.find("resilient.consumer.events.failed")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isZero();
    }

    @Test
    void registerPartition_shouldCreateProcessingLatencyTimer() {
        exporter.registerPartition(PARTITION_0);

        Timer timer = registry.find("resilient.consumer.processing.latency")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isZero();
    }

    @Test
    void registerPartition_shouldCreateReorderBufferDepthGauge() {
        exporter.registerPartition(PARTITION_0);

        Gauge gauge = registry.find("resilient.consumer.reorder.buffer.depth")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isZero();
    }

    @Test
    void registerPartition_shouldCreateDeduplicationCacheSizeGauge() {
        exporter.registerPartition(PARTITION_0);

        Gauge gauge = registry.find("resilient.consumer.dedup.cache.size")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isZero();
    }

    @Test
    void registerPartition_shouldBeIdempotent() {
        exporter.registerPartition(PARTITION_0);
        exporter.registerPartition(PARTITION_0);

        // Should only have one set of meters, not duplicates
        long count = registry.find("resilient.consumer.events.processed")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .meters().size();
        assertThat(count).isEqualTo(1);
    }

    // --- unregisterPartition removes all meters ---

    @Test
    void unregisterPartition_shouldRemoveAllMeters() {
        exporter.registerPartition(PARTITION_0);
        exporter.unregisterPartition(PARTITION_0);

        assertThat(registry.find("resilient.consumer.circuit.breaker.state")
                .tag("topic", "test-topic").tag("partition", "0").gauge()).isNull();
        assertThat(registry.find("resilient.consumer.events.processed")
                .tag("topic", "test-topic").tag("partition", "0").counter()).isNull();
        assertThat(registry.find("resilient.consumer.events.failed")
                .tag("topic", "test-topic").tag("partition", "0").counter()).isNull();
        assertThat(registry.find("resilient.consumer.processing.latency")
                .tag("topic", "test-topic").tag("partition", "0").timer()).isNull();
        assertThat(registry.find("resilient.consumer.reorder.buffer.depth")
                .tag("topic", "test-topic").tag("partition", "0").gauge()).isNull();
        assertThat(registry.find("resilient.consumer.dedup.cache.size")
                .tag("topic", "test-topic").tag("partition", "0").gauge()).isNull();
    }

    @Test
    void unregisterPartition_shouldNotAffectOtherPartitions() {
        exporter.registerPartition(PARTITION_0);
        exporter.registerPartition(PARTITION_1);

        exporter.unregisterPartition(PARTITION_0);

        // Partition 1 meters should still exist
        assertThat(registry.find("resilient.consumer.events.processed")
                .tag("topic", "test-topic").tag("partition", "1").counter()).isNotNull();
    }

    @Test
    void unregisterPartition_forUnregisteredPartition_shouldNotThrow() {
        // Should not throw when unregistering a partition that was never registered
        exporter.unregisterPartition(PARTITION_0);
    }

    // --- recordProcessingLatency records to timer ---

    @Test
    void recordProcessingLatency_shouldRecordToTimer() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordProcessingLatency(PARTITION_0, Duration.ofMillis(50));

        Timer timer = registry.find("resilient.consumer.processing.latency")
                .tag("topic", "test-topic").tag("partition", "0").timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(50.0);
    }

    @Test
    void recordProcessingLatency_forUnregisteredPartition_shouldNotThrow() {
        exporter.recordProcessingLatency(PARTITION_0, Duration.ofMillis(50));
        // No exception thrown
    }

    // --- recordProcessingSuccess increments counter ---

    @Test
    void recordProcessingSuccess_shouldIncrementCounter() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordProcessingSuccess(PARTITION_0);
        exporter.recordProcessingSuccess(PARTITION_0);

        Counter counter = registry.find("resilient.consumer.events.processed")
                .tag("topic", "test-topic").tag("partition", "0").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);
    }

    // --- recordProcessingFailure increments counter ---

    @Test
    void recordProcessingFailure_shouldIncrementCounter() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordProcessingFailure(PARTITION_0);
        exporter.recordProcessingFailure(PARTITION_0);
        exporter.recordProcessingFailure(PARTITION_0);

        Counter counter = registry.find("resilient.consumer.events.failed")
                .tag("topic", "test-topic").tag("partition", "0").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(3.0);
    }

    // --- recordCircuitBreakerTransition updates gauge and creates transition counter ---

    @Test
    void recordCircuitBreakerTransition_shouldUpdateGaugeToNewState() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordCircuitBreakerTransition(PARTITION_0, CircuitState.CLOSED, CircuitState.OPEN);

        Gauge gauge = registry.find("resilient.consumer.circuit.breaker.state")
                .tag("topic", "test-topic").tag("partition", "0").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(CircuitState.OPEN.ordinal());
    }

    @Test
    void recordCircuitBreakerTransition_shouldCreateTransitionCounter() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordCircuitBreakerTransition(PARTITION_0, CircuitState.CLOSED, CircuitState.OPEN);

        Counter counter = registry.find("resilient.consumer.circuit.breaker.transitions")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .tag("from_state", "CLOSED")
                .tag("to_state", "OPEN")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void recordCircuitBreakerTransition_multipleTransitions_shouldIncrementCounter() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordCircuitBreakerTransition(PARTITION_0, CircuitState.CLOSED, CircuitState.OPEN);
        exporter.recordCircuitBreakerTransition(PARTITION_0, CircuitState.CLOSED, CircuitState.OPEN);

        Counter counter = registry.find("resilient.consumer.circuit.breaker.transitions")
                .tag("from_state", "CLOSED")
                .tag("to_state", "OPEN")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);
    }

    // --- recordDlqRouting increments classification counter ---

    @Test
    void recordDlqRouting_shouldIncrementClassificationCounter() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordDlqRouting(PARTITION_0, ErrorClassification.TRANSIENT);

        Counter counter = registry.find("resilient.consumer.dlq.routed")
                .tag("topic", "test-topic")
                .tag("partition", "0")
                .tag("classification", "TRANSIENT")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void recordDlqRouting_differentClassifications_shouldTrackIndependently() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordDlqRouting(PARTITION_0, ErrorClassification.TRANSIENT);
        exporter.recordDlqRouting(PARTITION_0, ErrorClassification.PERMANENT);
        exporter.recordDlqRouting(PARTITION_0, ErrorClassification.PERMANENT);

        Counter transientCounter = registry.find("resilient.consumer.dlq.routed")
                .tag("classification", "TRANSIENT").counter();
        Counter permanentCounter = registry.find("resilient.consumer.dlq.routed")
                .tag("classification", "PERMANENT").counter();

        assertThat(transientCounter.count()).isEqualTo(1.0);
        assertThat(permanentCounter.count()).isEqualTo(2.0);
    }

    // --- recordReorderBufferDepth updates gauge ---

    @Test
    void recordReorderBufferDepth_shouldUpdateGauge() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordReorderBufferDepth(PARTITION_0, 42);

        Gauge gauge = registry.find("resilient.consumer.reorder.buffer.depth")
                .tag("topic", "test-topic").tag("partition", "0").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(42.0);
    }

    @Test
    void recordReorderBufferDepth_shouldReflectLatestValue() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordReorderBufferDepth(PARTITION_0, 10);
        exporter.recordReorderBufferDepth(PARTITION_0, 25);

        Gauge gauge = registry.find("resilient.consumer.reorder.buffer.depth")
                .tag("topic", "test-topic").tag("partition", "0").gauge();
        assertThat(gauge.value()).isEqualTo(25.0);
    }

    // --- recordDeduplicationCacheSize updates gauge ---

    @Test
    void recordDeduplicationCacheSize_shouldUpdateGauge() {
        exporter.registerPartition(PARTITION_0);

        exporter.recordDeduplicationCacheSize(PARTITION_0, 1000L);

        Gauge gauge = registry.find("resilient.consumer.dedup.cache.size")
                .tag("topic", "test-topic").tag("partition", "0").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(1000.0);
    }

    // --- getHealthStatus returns correct data ---

    @Test
    void getHealthStatus_shouldReturnAssignedPartitions() {
        exporter.registerPartition(PARTITION_0);
        exporter.registerPartition(PARTITION_1);

        HealthStatus status = exporter.getHealthStatus();

        assertThat(status.assignedPartitions()).containsExactlyInAnyOrder(PARTITION_0, PARTITION_1);
    }

    @Test
    void getHealthStatus_shouldReturnCircuitBreakerStates() {
        exporter.registerPartition(PARTITION_0);
        exporter.recordCircuitBreakerTransition(PARTITION_0, CircuitState.CLOSED, CircuitState.OPEN);

        HealthStatus status = exporter.getHealthStatus();

        assertThat(status.circuitBreakerStates()).containsEntry(PARTITION_0, CircuitState.OPEN);
    }

    @Test
    void getHealthStatus_shouldReturnBufferDepths() {
        exporter.registerPartition(PARTITION_0);
        exporter.recordReorderBufferDepth(PARTITION_0, 15);

        HealthStatus status = exporter.getHealthStatus();

        assertThat(status.reorderBufferDepths()).containsEntry(PARTITION_0, 15);
    }

    @Test
    void getHealthStatus_shouldReturnConsumerGroupStatus() {
        exporter.setConsumerGroupStatus("STABLE");

        HealthStatus status = exporter.getHealthStatus();

        assertThat(status.consumerGroupStatus()).isEqualTo("STABLE");
    }

    @Test
    void getHealthStatus_defaultConsumerGroupStatus_shouldBeUnknown() {
        HealthStatus status = exporter.getHealthStatus();

        assertThat(status.consumerGroupStatus()).isEqualTo("UNKNOWN");
    }

    // --- getRegisteredPartitions ---

    @Test
    void getRegisteredPartitions_shouldReturnAllRegistered() {
        exporter.registerPartition(PARTITION_0);
        exporter.registerPartition(PARTITION_1);

        Set<TopicPartition> registered = exporter.getRegisteredPartitions();

        assertThat(registered).containsExactlyInAnyOrder(PARTITION_0, PARTITION_1);
    }

    @Test
    void getRegisteredPartitions_afterUnregister_shouldNotContainRemoved() {
        exporter.registerPartition(PARTITION_0);
        exporter.registerPartition(PARTITION_1);
        exporter.unregisterPartition(PARTITION_0);

        Set<TopicPartition> registered = exporter.getRegisteredPartitions();

        assertThat(registered).containsExactly(PARTITION_1);
    }

    @Test
    void getRegisteredPartitions_whenEmpty_shouldReturnEmptySet() {
        Set<TopicPartition> registered = exporter.getRegisteredPartitions();

        assertThat(registered).isEmpty();
    }
}
