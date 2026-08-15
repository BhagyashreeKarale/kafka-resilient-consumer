package com.framework.resilient.metrics;

import com.framework.resilient.circuitbreaker.CircuitState;
import com.framework.resilient.dlq.ErrorClassification;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer registry integration with per-partition gauges, counters, and timers.
 * Registers and manages metrics for each assigned partition, providing observability
 * into circuit breaker state, processing performance, and component health.
 */
public class MetricsExporter {

    private static final String METRIC_PREFIX = "resilient.consumer";
    private static final String TAG_TOPIC = "topic";
    private static final String TAG_PARTITION = "partition";
    private static final String TAG_CLASSIFICATION = "classification";
    private static final String TAG_FROM_STATE = "from_state";
    private static final String TAG_TO_STATE = "to_state";

    private final MeterRegistry registry;
    private final ConcurrentHashMap<TopicPartition, PartitionMetrics> partitionMetrics;
    private final ConcurrentHashMap<TopicPartition, AtomicInteger> circuitBreakerStateValues;
    private final ConcurrentHashMap<TopicPartition, AtomicInteger> reorderBufferDepthValues;
    private final ConcurrentHashMap<TopicPartition, AtomicLong> deduplicationCacheSizeValues;
    private final ConcurrentHashMap<TopicPartition, Map<ErrorClassification, Counter>> dlqCounters;

    private volatile String consumerGroupStatus = "UNKNOWN";

    public MetricsExporter(MeterRegistry registry) {
        this.registry = registry;
        this.partitionMetrics = new ConcurrentHashMap<>();
        this.circuitBreakerStateValues = new ConcurrentHashMap<>();
        this.reorderBufferDepthValues = new ConcurrentHashMap<>();
        this.deduplicationCacheSizeValues = new ConcurrentHashMap<>();
        this.dlqCounters = new ConcurrentHashMap<>();
    }

    /**
     * Registers all meters (gauges, counters, timers) for the specified partition.
     * Metrics are tagged with topic and partition for per-partition identification.
     *
     * @param partition the Kafka topic partition to register metrics for
     */
    public void registerPartition(TopicPartition partition) {
        if (partitionMetrics.containsKey(partition)) {
            return;
        }

        Tags tags = Tags.of(TAG_TOPIC, partition.topic(), TAG_PARTITION, String.valueOf(partition.partition()));

        // Atomic holders for gauge values
        AtomicInteger cbStateValue = new AtomicInteger(CircuitState.CLOSED.ordinal());
        circuitBreakerStateValues.put(partition, cbStateValue);

        AtomicInteger bufferDepthValue = new AtomicInteger(0);
        reorderBufferDepthValues.put(partition, bufferDepthValue);

        AtomicLong cacheSizeValue = new AtomicLong(0);
        deduplicationCacheSizeValues.put(partition, cacheSizeValue);

        // Register gauges
        Gauge circuitBreakerState = Gauge.builder(METRIC_PREFIX + ".circuit.breaker.state", cbStateValue, AtomicInteger::get)
                .tags(tags)
                .description("Current circuit breaker state ordinal (0=CLOSED, 1=OPEN, 2=HALF_OPEN)")
                .register(registry);

        Gauge reorderBufferDepth = Gauge.builder(METRIC_PREFIX + ".reorder.buffer.depth", bufferDepthValue, AtomicInteger::get)
                .tags(tags)
                .description("Current number of events buffered in the reorder buffer")
                .register(registry);

        Gauge deduplicationCacheSize = Gauge.builder(METRIC_PREFIX + ".dedup.cache.size", cacheSizeValue, AtomicLong::get)
                .tags(tags)
                .description("Current number of idempotency keys in the deduplication cache")
                .register(registry);

        // Register counters
        Counter processedEvents = Counter.builder(METRIC_PREFIX + ".events.processed")
                .tags(tags)
                .description("Total number of successfully processed events")
                .register(registry);

        Counter failedEvents = Counter.builder(METRIC_PREFIX + ".events.failed")
                .tags(tags)
                .description("Total number of failed events")
                .register(registry);

        // Register timer with p50/p95/p99 histogram percentiles
        Timer processingLatency = Timer.builder(METRIC_PREFIX + ".processing.latency")
                .tags(tags)
                .description("Event processing latency")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);

        // Register DLQ counters per error classification
        Map<ErrorClassification, Counter> classificationCounters = new HashMap<>();
        for (ErrorClassification classification : ErrorClassification.values()) {
            Counter dlqCounter = Counter.builder(METRIC_PREFIX + ".dlq.routed")
                    .tags(tags.and(TAG_CLASSIFICATION, classification.name()))
                    .description("Number of events routed to DLQ by classification")
                    .register(registry);
            classificationCounters.put(classification, dlqCounter);
        }
        dlqCounters.put(partition, classificationCounters);

        // Store partition metrics record
        PartitionMetrics metrics = new PartitionMetrics(
                circuitBreakerState,
                processedEvents,
                failedEvents,
                processingLatency,
                reorderBufferDepth,
                deduplicationCacheSize
        );
        partitionMetrics.put(partition, metrics);
    }

    /**
     * Unregisters and removes all meters for the specified partition.
     *
     * @param partition the Kafka topic partition to unregister metrics for
     */
    public void unregisterPartition(TopicPartition partition) {
        PartitionMetrics metrics = partitionMetrics.remove(partition);
        if (metrics == null) {
            return;
        }

        Tags tags = Tags.of(TAG_TOPIC, partition.topic(), TAG_PARTITION, String.valueOf(partition.partition()));

        // Remove all registered meters for this partition
        registry.find(METRIC_PREFIX + ".circuit.breaker.state").tags(tags).meters().forEach(registry::remove);
        registry.find(METRIC_PREFIX + ".reorder.buffer.depth").tags(tags).meters().forEach(registry::remove);
        registry.find(METRIC_PREFIX + ".dedup.cache.size").tags(tags).meters().forEach(registry::remove);
        registry.find(METRIC_PREFIX + ".events.processed").tags(tags).meters().forEach(registry::remove);
        registry.find(METRIC_PREFIX + ".events.failed").tags(tags).meters().forEach(registry::remove);
        registry.find(METRIC_PREFIX + ".processing.latency").tags(tags).meters().forEach(registry::remove);

        // Remove DLQ counters for all classifications
        for (ErrorClassification classification : ErrorClassification.values()) {
            Tags dlqTags = tags.and(TAG_CLASSIFICATION, classification.name());
            registry.find(METRIC_PREFIX + ".dlq.routed").tags(dlqTags).meters().forEach(registry::remove);
        }

        // Clean up atomic holders
        circuitBreakerStateValues.remove(partition);
        reorderBufferDepthValues.remove(partition);
        deduplicationCacheSizeValues.remove(partition);
        dlqCounters.remove(partition);
    }

    /**
     * Records processing latency for a successfully or failed event on the specified partition.
     *
     * @param partition the partition the event belongs to
     * @param latency   the duration of event processing
     */
    public void recordProcessingLatency(TopicPartition partition, Duration latency) {
        PartitionMetrics metrics = partitionMetrics.get(partition);
        if (metrics != null) {
            metrics.processingLatency().record(latency);
        }
    }

    /**
     * Records a successful event processing on the specified partition.
     *
     * @param partition the partition the event was processed on
     */
    public void recordProcessingSuccess(TopicPartition partition) {
        PartitionMetrics metrics = partitionMetrics.get(partition);
        if (metrics != null) {
            metrics.processedEvents().increment();
        }
    }

    /**
     * Records a failed event processing on the specified partition.
     *
     * @param partition the partition the event failed on
     */
    public void recordProcessingFailure(TopicPartition partition) {
        PartitionMetrics metrics = partitionMetrics.get(partition);
        if (metrics != null) {
            metrics.failedEvents().increment();
        }
    }

    /**
     * Records a circuit breaker state transition for the specified partition.
     * Updates the gauge value and increments a transition counter.
     *
     * @param partition the affected partition
     * @param from      the previous circuit breaker state
     * @param to        the new circuit breaker state
     */
    public void recordCircuitBreakerTransition(TopicPartition partition, CircuitState from, CircuitState to) {
        AtomicInteger stateValue = circuitBreakerStateValues.get(partition);
        if (stateValue != null) {
            stateValue.set(to.ordinal());
        }

        // Record the transition as a counter for tracking state change frequency
        Tags tags = Tags.of(
                TAG_TOPIC, partition.topic(),
                TAG_PARTITION, String.valueOf(partition.partition()),
                TAG_FROM_STATE, from.name(),
                TAG_TO_STATE, to.name()
        );
        Counter.builder(METRIC_PREFIX + ".circuit.breaker.transitions")
                .tags(tags)
                .description("Number of circuit breaker state transitions")
                .register(registry)
                .increment();
    }

    /**
     * Records a DLQ routing event for the specified partition and error classification.
     *
     * @param partition      the partition the event originated from
     * @param classification the error classification that triggered DLQ routing
     */
    public void recordDlqRouting(TopicPartition partition, ErrorClassification classification) {
        Map<ErrorClassification, Counter> counters = dlqCounters.get(partition);
        if (counters != null) {
            Counter counter = counters.get(classification);
            if (counter != null) {
                counter.increment();
            }
        }
    }

    /**
     * Records the current reorder buffer depth for the specified partition.
     *
     * @param partition the partition to update buffer depth for
     * @param depth     the current buffer depth
     */
    public void recordReorderBufferDepth(TopicPartition partition, int depth) {
        AtomicInteger depthValue = reorderBufferDepthValues.get(partition);
        if (depthValue != null) {
            depthValue.set(depth);
        }
    }

    /**
     * Records the current deduplication cache size for the specified partition.
     *
     * @param partition the partition to update cache size for
     * @param size      the current cache size (number of idempotency keys)
     */
    public void recordDeduplicationCacheSize(TopicPartition partition, long size) {
        AtomicLong sizeValue = deduplicationCacheSizeValues.get(partition);
        if (sizeValue != null) {
            sizeValue.set(size);
        }
    }

    /**
     * Returns the current health status including consumer group status,
     * partition assignments, circuit breaker states, and reorder buffer depths.
     *
     * @return the aggregated health status
     */
    public HealthStatus getHealthStatus() {
        Set<TopicPartition> assignedPartitions = new HashSet<>(partitionMetrics.keySet());

        Map<TopicPartition, CircuitState> cbStates = new HashMap<>();
        for (Map.Entry<TopicPartition, AtomicInteger> entry : circuitBreakerStateValues.entrySet()) {
            int ordinal = entry.getValue().get();
            cbStates.put(entry.getKey(), CircuitState.values()[ordinal]);
        }

        Map<TopicPartition, Integer> bufferDepths = new HashMap<>();
        for (Map.Entry<TopicPartition, AtomicInteger> entry : reorderBufferDepthValues.entrySet()) {
            bufferDepths.put(entry.getKey(), entry.getValue().get());
        }

        return new HealthStatus(consumerGroupStatus, assignedPartitions, cbStates, bufferDepths);
    }

    /**
     * Updates the consumer group status reported by health checks.
     *
     * @param status the current consumer group status (e.g., "STABLE", "REBALANCING")
     */
    public void setConsumerGroupStatus(String status) {
        this.consumerGroupStatus = status;
    }

    /**
     * Returns the partition metrics for the specified partition, or null if not registered.
     *
     * @param partition the partition to look up
     * @return the PartitionMetrics, or null if the partition is not registered
     */
    public PartitionMetrics getPartitionMetrics(TopicPartition partition) {
        return partitionMetrics.get(partition);
    }

    /**
     * Returns all currently registered partitions.
     *
     * @return set of registered partitions
     */
    public Set<TopicPartition> getRegisteredPartitions() {
        return new HashSet<>(partitionMetrics.keySet());
    }
}
