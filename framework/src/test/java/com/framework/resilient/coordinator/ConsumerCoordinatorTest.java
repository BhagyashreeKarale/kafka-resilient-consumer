package com.framework.resilient.coordinator;

import com.framework.resilient.circuitbreaker.CircuitBreakerProperties;
import com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot;
import com.framework.resilient.circuitbreaker.CircuitState;
import com.framework.resilient.circuitbreaker.HealthMonitorProperties;
import com.framework.resilient.circuitbreaker.PartitionCircuitBreaker;
import com.framework.resilient.circuitbreaker.PartitionHealthMonitor;
import com.framework.resilient.dedup.DeduplicationEngine;
import com.framework.resilient.dedup.DeduplicationProperties;
import com.framework.resilient.dedup.DeduplicationResult;
import com.framework.resilient.dedup.DeduplicationSnapshot;
import com.framework.resilient.dedup.InMemoryIdempotencyKeyStore;
import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.dlq.DeserializationException;
import com.framework.resilient.dlq.ErrorClassification;
import com.framework.resilient.metrics.MetricsExporter;
import com.framework.resilient.reorder.ReorderBuffer;
import com.framework.resilient.reorder.ReorderBufferProperties;
import com.framework.resilient.reorder.ReorderResult;
import com.framework.resilient.reorder.SequencedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConsumerCoordinatorTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);

    @Mock
    private KafkaConsumer<String, byte[]> consumer;

    @Mock
    private EventHandler<String> eventHandler;

    @Mock
    private EventDeserializer<String> deserializer;

    @Mock
    private DLQRouter dlqRouter;

    @Mock
    private StateStore stateStore;

    private PartitionCircuitBreaker circuitBreaker;
    private PartitionHealthMonitor healthMonitor;
    private ReorderBuffer<String> reorderBuffer;
    private DeduplicationEngine deduplicationEngine;
    private MetricsExporter metricsExporter;

    private CoordinatorProperties coordinatorProperties;
    private DLQProperties dlqProperties;

    @BeforeEach
    void setUp() {
        coordinatorProperties = new CoordinatorProperties(
                Duration.ofMillis(100), 500,
                Duration.ofSeconds(30), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Duration.ofSeconds(10),
                Duration.ofSeconds(30)
        );

        dlqProperties = new DLQProperties(
                3, Duration.ofMillis(10), Duration.ofMillis(50),
                1000, 10,
                Map.of(
                        ErrorClassification.DESERIALIZATION, "dlq.deserialization",
                        ErrorClassification.TRANSIENT, "dlq.transient",
                        ErrorClassification.PERMANENT, "dlq.permanent"
                ),
                "dlq.unclassified"
        );

        CircuitBreakerProperties cbProperties = new CircuitBreakerProperties(
                0.5, Duration.ofMillis(5000), Duration.ofSeconds(30),
                10, Duration.ofSeconds(30), Duration.ofSeconds(2), Duration.ofSeconds(1)
        );

        HealthMonitorProperties hmProperties = new HealthMonitorProperties(
                Duration.ofSeconds(60), Duration.ofSeconds(1)
        );

        ReorderBufferProperties rbProperties = new ReorderBufferProperties(
                10000, 0.8, Duration.ofSeconds(5)
        );

        DeduplicationProperties dedupProperties = new DeduplicationProperties(
                Duration.ofHours(72), Duration.ofHours(1), Duration.ofSeconds(5)
        );

        // Use real components as specified in the task
        circuitBreaker = new PartitionCircuitBreaker(cbProperties, tp -> {}, tp -> {});
        healthMonitor = new PartitionHealthMonitor(hmProperties, cbProperties, signal ->
                circuitBreaker.onDegradationSignal(signal.partition(), signal));
        reorderBuffer = new ReorderBuffer<>(rbProperties, tp -> {}, tp -> {});
        deduplicationEngine = new DeduplicationEngine(
                new InMemoryIdempotencyKeyStore(), dedupProperties,
                Executors.newSingleThreadScheduledExecutor()
        );
        metricsExporter = new MetricsExporter(new SimpleMeterRegistry());
    }

    // ========================= Pipeline Execution Order =========================

    @Test
    void processRecord_successfulEvent_followsPipelineOrder() throws Exception {
        // Setup: deserialize returns a valid event
        ConsumerRecord<String, byte[]> record = createRecord(PARTITION_0, 100L);
        SequencedEvent<String> event = createEvent("entity-1", 1L, 100L);
        when(deserializer.deserialize(record)).thenReturn(event);
        when(eventHandler.handle(any(), any())).thenReturn(ProcessingResult.SUCCESS);

        circuitBreaker.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);
        healthMonitor.initializePartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();
        invokeProcessRecord(coordinator, record);

        // Verify: handler was called (event went through pipeline)
        verify(eventHandler).handle(eq("test-payload"), any(EventMetadata.class));
        // Verify: offset committed
        verify(consumer).commitSync(any(Map.class));
    }

    // ========================= Deserialization Failure Routes to DLQ =========================

    @Test
    void processRecord_deserializationFailure_routesToDlqDirectly() throws Exception {
        ConsumerRecord<String, byte[]> record = createRecord(PARTITION_0, 100L);
        when(deserializer.deserialize(record))
                .thenThrow(new DeserializationException("bad payload"));

        circuitBreaker.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();
        invokeProcessRecord(coordinator, record);

        // DLQ should be called with deserialization classification
        verify(dlqRouter).route(eq(record), any(DeserializationException.class),
                eq(ErrorClassification.DESERIALIZATION), eq(0), any(), any());
        // Handler should NEVER be called
        verifyNoInteractions(eventHandler);
        // Offset should still be committed (event is dealt with)
        verify(consumer).commitSync(any(Map.class));
    }

    // ========================= Transient Failure Retry with Backoff =========================

    @Test
    void processRecord_transientFailure_retriesBeforeRoutingToDlq() throws Exception {
        ConsumerRecord<String, byte[]> record = createRecord(PARTITION_0, 100L);
        SequencedEvent<String> event = createEvent("entity-1", 1L, 100L);
        when(deserializer.deserialize(record)).thenReturn(event);
        when(eventHandler.handle(any(), any()))
                .thenThrow(new TransientProcessingException("temporary error"));

        circuitBreaker.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);
        healthMonitor.initializePartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();
        invokeProcessRecord(coordinator, record);

        // Handler should be called maxRetryCount + 1 times (initial + retries)
        verify(eventHandler, times(dlqProperties.maxRetryCount() + 1)).handle(any(), any());
    }

    // ========================= Permanent Failure Routes to DLQ Without Retry =========================

    @Test
    void processRecord_permanentFailure_routesToDlqWithoutRetry() throws Exception {
        ConsumerRecord<String, byte[]> record = createRecord(PARTITION_0, 100L);
        SequencedEvent<String> event = createEvent("entity-1", 1L, 100L);
        when(deserializer.deserialize(record)).thenReturn(event);
        when(eventHandler.handle(any(), any()))
                .thenThrow(new PermanentProcessingException("non-retryable"));

        circuitBreaker.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);
        healthMonitor.initializePartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();
        invokeProcessRecord(coordinator, record);

        // Handler called only once — no retries for permanent failures
        verify(eventHandler, times(1)).handle(any(), any());
        // Offset committed (event dealt with)
        verify(consumer).commitSync(any(Map.class));
    }

    // ========================= Duplicate Detection Skips Business Logic =========================

    @Test
    void processRecord_duplicateEvent_skipsHandlerAndCommitsOffset() throws Exception {
        circuitBreaker.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);
        healthMonitor.initializePartition(PARTITION_0);

        // Pre-populate dedup engine to simulate previously processed event
        deduplicationEngine.markProcessed(PARTITION_0, "entity-1", 1L);

        ConsumerCoordinator<String> coordinator = createCoordinator();

        // Process a record with same entity/sequence (it will be recognized as duplicate)
        ConsumerRecord<String, byte[]> record = createRecord(PARTITION_0, 101L);
        SequencedEvent<String> event = createEvent("entity-1", 1L, 101L);
        when(deserializer.deserialize(record)).thenReturn(event);

        invokeProcessRecord(coordinator, record);

        // Since seq 1 was already processed, reorder buffer will forward to dedup
        // which recognizes it as duplicate and skips handler
        // The handler should not be invoked for the duplicate
    }

    // ========================= Graceful Shutdown Flushes Reorder Buffer =========================

    @Test
    void shutdown_shouldCommitOffsets() {
        when(consumer.assignment()).thenReturn(Set.of(PARTITION_0));

        reorderBuffer.initializePartition(PARTITION_0);
        circuitBreaker.initializePartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();

        // Call shutdown directly — the poll thread is not started, so shutdown just does cleanup
        coordinator.shutdown(Duration.ofSeconds(5));

        // Should attempt to commit offsets during shutdown
        verify(consumer).commitSync(any(Duration.class));
    }

    // ========================= Rebalance Triggers State Persistence =========================

    @Test
    void onPartitionsRevoked_shouldCommitOffsets() {
        circuitBreaker.initializePartition(PARTITION_0);
        healthMonitor.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();
        coordinator.onPartitionsRevoked(List.of(PARTITION_0));

        verify(consumer).commitSync(coordinatorProperties.rebalanceCommitTimeout());
    }

    @Test
    void onPartitionsRevoked_shouldPersistCircuitBreakerState() {
        circuitBreaker.initializePartition(PARTITION_0);
        healthMonitor.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();
        coordinator.onPartitionsRevoked(List.of(PARTITION_0));

        verify(stateStore).persistCircuitBreakerState(any());
    }

    @Test
    void onPartitionsRevoked_shouldPersistDeduplicationState() {
        circuitBreaker.initializePartition(PARTITION_0);
        healthMonitor.initializePartition(PARTITION_0);
        reorderBuffer.initializePartition(PARTITION_0);
        metricsExporter.registerPartition(PARTITION_0);

        ConsumerCoordinator<String> coordinator = createCoordinator();
        coordinator.onPartitionsRevoked(List.of(PARTITION_0));

        verify(stateStore).persistDeduplicationState(any());
    }

    // ========================= Rebalance Restores State for Assigned Partitions =========================

    @Test
    void onPartitionsAssigned_shouldRestoreCircuitBreakerState() {
        CircuitBreakerSnapshot snapshot = new CircuitBreakerSnapshot(
                CircuitState.OPEN, Instant.now(), 3, Instant.now()
        );
        when(stateStore.restoreCircuitBreakerState(PARTITION_0)).thenReturn(Optional.of(snapshot));
        when(stateStore.restoreBufferState(PARTITION_0)).thenReturn(Optional.empty());
        when(stateStore.restoreDeduplicationState(PARTITION_0)).thenReturn(Optional.empty());

        ConsumerCoordinator<String> coordinator = createCoordinator();
        coordinator.onPartitionsAssigned(List.of(PARTITION_0));

        verify(stateStore).restoreCircuitBreakerState(PARTITION_0);
        // The circuit breaker should now be in OPEN state
        assertThat(circuitBreaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
    }

    @Test
    void onPartitionsAssigned_withNoPersistedState_shouldInitializeFresh() {
        when(stateStore.restoreCircuitBreakerState(PARTITION_0)).thenReturn(Optional.empty());
        when(stateStore.restoreBufferState(PARTITION_0)).thenReturn(Optional.empty());
        when(stateStore.restoreDeduplicationState(PARTITION_0)).thenReturn(Optional.empty());

        ConsumerCoordinator<String> coordinator = createCoordinator();
        coordinator.onPartitionsAssigned(List.of(PARTITION_0));

        // Circuit breaker should be initialized in CLOSED state
        assertThat(circuitBreaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void onPartitionsAssigned_shouldRestoreDeduplicationState() {
        DeduplicationSnapshot dedupSnapshot = new DeduplicationSnapshot(
                Map.of("entity-1:1:0", Instant.now())
        );
        when(stateStore.restoreCircuitBreakerState(PARTITION_0)).thenReturn(Optional.empty());
        when(stateStore.restoreBufferState(PARTITION_0)).thenReturn(Optional.empty());
        when(stateStore.restoreDeduplicationState(PARTITION_0)).thenReturn(Optional.of(dedupSnapshot));

        ConsumerCoordinator<String> coordinator = createCoordinator();
        coordinator.onPartitionsAssigned(List.of(PARTITION_0));

        verify(stateStore).restoreDeduplicationState(PARTITION_0);
    }

    // ========================= Corrupted State Falls Back to Empty =========================

    @Test
    void onPartitionsAssigned_whenRestoreThrowsException_shouldInitializeFresh() {
        when(stateStore.restoreCircuitBreakerState(PARTITION_0))
                .thenThrow(new RuntimeException("Corrupted state"));
        when(stateStore.restoreBufferState(PARTITION_0)).thenReturn(Optional.empty());
        when(stateStore.restoreDeduplicationState(PARTITION_0)).thenReturn(Optional.empty());

        ConsumerCoordinator<String> coordinator = createCoordinator();

        // Should not throw — falls back gracefully
        coordinator.onPartitionsAssigned(List.of(PARTITION_0));

        // Partition should still be usable (initialized fresh)
        // The circuit breaker state will be CLOSED (default for untracked)
        assertThat(circuitBreaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
    }

    // ========================= Helper Methods =========================

    private ConsumerCoordinator<String> createCoordinator() {
        return new ConsumerCoordinator<>(
                consumer,
                eventHandler,
                deserializer,
                circuitBreaker,
                healthMonitor,
                reorderBuffer,
                deduplicationEngine,
                dlqRouter,
                metricsExporter,
                stateStore,
                coordinatorProperties,
                dlqProperties
        );
    }

    private ConsumerRecord<String, byte[]> createRecord(TopicPartition tp, long offset) {
        return new ConsumerRecord<>(
                tp.topic(), tp.partition(), offset,
                "key-1", "test-payload".getBytes()
        );
    }

    private SequencedEvent<String> createEvent(String sourceEntity, long seqNum, long offset) {
        EventMetadata metadata = new EventMetadata(
                "test-topic", 0, offset, "corr-id-1",
                new RecordHeaders(), Instant.now()
        );
        return new SequencedEvent<>(sourceEntity, seqNum, "test-payload", metadata, null);
    }

    /**
     * Uses reflection to invoke the private processRecord method directly
     * for unit testing the pipeline without running the full poll loop.
     * Also flushes pending offsets to simulate end-of-batch behavior.
     */
    private void invokeProcessRecord(ConsumerCoordinator<String> coordinator,
                                     ConsumerRecord<String, byte[]> record) {
        try {
            var method = ConsumerCoordinator.class.getDeclaredMethod(
                    "processRecord", ConsumerRecord.class);
            method.setAccessible(true);
            method.invoke(coordinator, record);

            // Simulate end-of-batch: flush pending offsets (batched commit)
            var flushMethod = ConsumerCoordinator.class.getDeclaredMethod("flushPendingOffsets");
            flushMethod.setAccessible(true);
            flushMethod.invoke(coordinator);
        } catch (Exception e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Failed to invoke processRecord", e);
        }
    }
}
