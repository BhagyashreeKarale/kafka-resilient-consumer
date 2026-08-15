package com.framework.resilient.coordinator;

import com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot;
import com.framework.resilient.circuitbreaker.CircuitState;
import com.framework.resilient.circuitbreaker.PartitionCircuitBreaker;
import com.framework.resilient.circuitbreaker.PartitionHealthMonitor;
import com.framework.resilient.dedup.DeduplicationEngine;
import com.framework.resilient.dedup.DeduplicationResult;
import com.framework.resilient.dedup.DeduplicationSnapshot;
import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.dlq.DeserializationException;
import com.framework.resilient.dlq.ErrorClassification;
import com.framework.resilient.metrics.MetricsExporter;
import com.framework.resilient.reorder.BufferSnapshot;
import com.framework.resilient.reorder.ReorderBuffer;
import com.framework.resilient.reorder.ReorderResult;
import com.framework.resilient.reorder.SequencedEvent;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Central orchestrator for the resilient Kafka consumer framework.
 * Manages the poll loop, processing pipeline, pause/resume coordination,
 * rebalance handling, and graceful shutdown.
 *
 * <p>Processing pipeline per record:
 * <ol>
 *   <li>Deserialize (failure → DLQ DESERIALIZATION)</li>
 *   <li>Circuit breaker check (OPEN → skip)</li>
 *   <li>Reorder buffer submission</li>
 *   <li>Deduplication check (DUPLICATE → commit offset, skip)</li>
 *   <li>Business handler invocation with retry</li>
 *   <li>Offset commit and health metrics recording</li>
 * </ol>
 *
 * @param <T> the deserialized event payload type
 */
public class ConsumerCoordinator<T> implements Lifecycle, ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    private static final String MDC_CORRELATION_ID = "correlationId";
    private static final String MDC_PARTITION = "partition";
    private static final String MDC_TOPIC = "topic";
    private static final String MDC_OFFSET = "offset";
    private static final String MDC_PIPELINE_STAGE = "pipelineStage";

    private final KafkaConsumer<String, byte[]> consumer;
    private final EventHandler<T> eventHandler;
    private final EventDeserializer<T> deserializer;
    private final PartitionCircuitBreaker circuitBreaker;
    private final PartitionHealthMonitor healthMonitor;
    private final ReorderBuffer<T> reorderBuffer;
    private final DeduplicationEngine deduplicationEngine;
    private final DLQRouter dlqRouter;
    private final MetricsExporter metricsExporter;
    private final StateStore stateStore;
    private final CoordinatorProperties properties;
    private final DLQProperties dlqProperties;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread pollThread;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final Collection<String> topics;

    // Batched offset tracking — commit at end of poll batch, not per-record
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap<>();

    // Thread-safe command queue: other threads enqueue actions here, poll thread drains and executes.
    // This ensures all KafkaConsumer interactions happen on the poll thread (KafkaConsumer is NOT thread-safe).
    private final ConcurrentLinkedQueue<Runnable> consumerActions = new ConcurrentLinkedQueue<>();

    // Thread-safe snapshot of assigned partitions — updated on poll thread, read by health-check threads.
    private final AtomicReference<Set<TopicPartition>> assignedPartitionsSnapshot =
            new AtomicReference<>(Set.of());

    /**
     * Constructs a ConsumerCoordinator with all required dependencies.
     *
     * @param consumer             the Kafka consumer (caller owns lifecycle before start)
     * @param eventHandler         business logic handler (domain-specific)
     * @param deserializer         raw bytes to SequencedEvent converter
     * @param circuitBreaker       per-partition circuit breaker
     * @param healthMonitor        partition health tracking
     * @param reorderBuffer        out-of-order event buffering
     * @param deduplicationEngine  idempotency key tracking
     * @param dlqRouter            dead letter queue routing
     * @param metricsExporter      Micrometer metrics
     * @param stateStore           pluggable state persistence
     * @param properties           coordinator configuration
     * @param dlqProperties        DLQ/retry configuration
     * @param topics               topics to subscribe to
     */
    public ConsumerCoordinator(
            KafkaConsumer<String, byte[]> consumer,
            EventHandler<T> eventHandler,
            EventDeserializer<T> deserializer,
            PartitionCircuitBreaker circuitBreaker,
            PartitionHealthMonitor healthMonitor,
            ReorderBuffer<T> reorderBuffer,
            DeduplicationEngine deduplicationEngine,
            DLQRouter dlqRouter,
            MetricsExporter metricsExporter,
            StateStore stateStore,
            CoordinatorProperties properties,
            DLQProperties dlqProperties,
            Collection<String> topics) {
        this.consumer = consumer;
        this.eventHandler = eventHandler;
        this.deserializer = deserializer;
        this.circuitBreaker = circuitBreaker;
        this.healthMonitor = healthMonitor;
        this.reorderBuffer = reorderBuffer;
        this.deduplicationEngine = deduplicationEngine;
        this.dlqRouter = dlqRouter;
        this.metricsExporter = metricsExporter;
        this.stateStore = stateStore;
        this.properties = properties;
        this.dlqProperties = dlqProperties;
        this.topics = topics != null ? List.copyOf(topics) : List.of();
    }

    /**
     * Backwards-compatible constructor without topics parameter.
     * Topics must be set via {@link #subscribe(Collection)} before {@link #start()}.
     */
    public ConsumerCoordinator(
            KafkaConsumer<String, byte[]> consumer,
            EventHandler<T> eventHandler,
            EventDeserializer<T> deserializer,
            PartitionCircuitBreaker circuitBreaker,
            PartitionHealthMonitor healthMonitor,
            ReorderBuffer<T> reorderBuffer,
            DeduplicationEngine deduplicationEngine,
            DLQRouter dlqRouter,
            MetricsExporter metricsExporter,
            StateStore stateStore,
            CoordinatorProperties properties,
            DLQProperties dlqProperties) {
        this(consumer, eventHandler, deserializer, circuitBreaker, healthMonitor,
                reorderBuffer, deduplicationEngine, dlqRouter, metricsExporter,
                stateStore, properties, dlqProperties, null);
    }

    // ========================= Lifecycle =========================

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("ConsumerCoordinator already running");
            return;
        }
        log.info("Starting ConsumerCoordinator");
        pollThread = new Thread(this::pollLoop, "resilient-consumer-poll");
        pollThread.setDaemon(true);
        pollThread.start();
        // Health-check callbacks enqueue actions to the command queue instead of calling
        // KafkaConsumer directly — preserving thread-safety (KafkaConsumer is NOT thread-safe).
        deduplicationEngine.startHealthCheck(
                partitions -> partitions.forEach(tp ->
                        consumerActions.add(() -> pausePartition(tp))),
                () -> assignedPartitionsSnapshot.get()
        );
    }

    @Override
    public void shutdown(Duration timeout) {
        log.info("Shutting down ConsumerCoordinator with timeout {}", timeout);
        running.set(false);

        // Stop background health-check threads FIRST — prevents them from enqueuing
        // actions to consumerActions after the poll thread exits.
        deduplicationEngine.stopHealthCheck();

        if (pollThread != null) {
            consumer.wakeup();
            try {
                pollThread.join(timeout.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted waiting for poll thread shutdown");
            }
        }

        if (pollThread != null && pollThread.isAlive()) {
            log.warn("Poll thread did not stop within timeout, forcing shutdown");
            forceShutdown();
        } else {
            performGracefulShutdown();
        }

        shutdownLatch.countDown();
        log.info("ConsumerCoordinator shutdown complete");
    }

    // ========================= Poll Loop =========================

    private void pollLoop() {
        try {
            // Subscribe with cooperative sticky assignor via this as rebalance listener
            if (!topics.isEmpty()) {
                consumer.subscribe(topics, this);
            } else if (consumer.subscription().isEmpty()) {
                log.error("ConsumerCoordinator started with no topics configured. "
                        + "Provide topics via constructor or call subscribe() before start().");
                running.set(false);
                return;
            }

            restoreState();

            while (running.get()) {
                try {
                    // Drain command queue — execute any actions enqueued by background threads
                    // (e.g., dedup health-check requesting pause). This ensures all KafkaConsumer
                    // interactions happen on the poll thread.
                    Runnable cmd;
                    while ((cmd = consumerActions.poll()) != null) {
                        try { cmd.run(); } catch (Exception e) {
                            log.warn("Consumer action failed", e);
                        }
                    }

                    ConsumerRecords<String, byte[]> records = consumer.poll(properties.pollTimeout());

                    // Update assignment snapshot for thread-safe reads by background threads
                    assignedPartitionsSnapshot.set(Set.copyOf(consumer.assignment()));

                    for (ConsumerRecord<String, byte[]> record : records) {
                        if (!running.get()) break;
                        processRecord(record);
                    }

                    // Batch commit: flush all accumulated offsets after processing the poll batch
                    flushPendingOffsets();

                    // Post-batch maintenance
                    circuitBreaker.evaluateCooldowns();
                    for (TopicPartition tp : consumer.assignment()) {
                        // Resume partitions whose circuit breaker transitioned to HALF_OPEN
                        if (circuitBreaker.getState(tp) == CircuitState.HALF_OPEN
                                && consumer.paused().contains(tp)) {
                            consumer.resume(Collections.singleton(tp));
                            log.info("Resumed partition {} for probe (HALF_OPEN)", tp);
                        }

                        List<SequencedEvent<T>> timedOut = reorderBuffer.releaseTimedOut(tp);
                        if (!timedOut.isEmpty()) {
                            processReleasedEvents(tp, timedOut);
                        }
                    }
                    dlqRouter.retryBuffered();

                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    if (running.get()) {
                        log.warn("Unexpected wakeup while still running");
                    }
                } catch (Exception e) {
                    log.error("Error in poll loop", e);
                    if (running.get()) {
                        try { Thread.sleep(1000); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        } finally {
            MDC.clear();
        }
    }

    // ========================= Record Processing Pipeline =========================

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        String correlationId = UUID.randomUUID().toString();

        MDC.put(MDC_CORRELATION_ID, correlationId);
        MDC.put(MDC_TOPIC, record.topic());
        MDC.put(MDC_PARTITION, String.valueOf(record.partition()));
        MDC.put(MDC_OFFSET, String.valueOf(record.offset()));

        try {
            // Stage 1: Deserialize
            MDC.put(MDC_PIPELINE_STAGE, "DESERIALIZE");
            SequencedEvent<T> event;
            try {
                event = deserializer.deserialize(record);
            } catch (DeserializationException e) {
                log.warn("Deserialization failed for record at offset {}", record.offset(), e);
                routeToDlq(record, e, ErrorClassification.DESERIALIZATION, 0, null, correlationId);
                trackOffset(partition, record.offset());
                return;
            }

            // Stage 2: Circuit breaker check
            MDC.put(MDC_PIPELINE_STAGE, "CIRCUIT_BREAKER_CHECK");
            if (!circuitBreaker.isPartitionHealthy(partition)) {
                // Partition is degraded — pause it so poll() stops returning records for it.
                // The partition will be resumed in post-batch maintenance when cooldown elapses
                // and the circuit breaker transitions to HALF_OPEN.
                if (!consumer.paused().contains(partition)) {
                    consumer.pause(Collections.singleton(partition));
                    log.info("Paused partition {} — circuit breaker OPEN", partition);
                }
                // Do NOT commit offset here — the record is not processed or DLQ'd.
                // When the partition resumes (HALF_OPEN probe), this record will be re-delivered.
                // This ensures no data loss: the message is retried after cooldown, not silently dropped.
                return;
            }

            // Stage 3: Reorder buffer
            MDC.put(MDC_PIPELINE_STAGE, "REORDER");
            ReorderResult<T> reorderResult = reorderBuffer.submit(partition, event);
            metricsExporter.recordReorderBufferDepth(partition, reorderBuffer.getBufferDepth(partition));

            // Process any released events (in-order or previously buffered)
            if (!reorderResult.releasedEvents().isEmpty()) {
                processReleasedEvents(partition, reorderResult.releasedEvents());
            }

            // Handle possible duplicates (seq <= lastProcessed)
            if (!reorderResult.possibleDuplicates().isEmpty()) {
                for (SequencedEvent<T> dupCandidate : reorderResult.possibleDuplicates()) {
                    MDC.put(MDC_PIPELINE_STAGE, "DEDUPLICATION");
                    DeduplicationResult dedupResult = deduplicationEngine.check(
                            partition, dupCandidate.sourceEntity(), dupCandidate.sequenceNumber());
                    if (dedupResult == DeduplicationResult.DUPLICATE) {
                        log.debug("Duplicate detected for entity={} seq={}", dupCandidate.sourceEntity(), dupCandidate.sequenceNumber());
                        // Use the duplicate candidate's own offset, not the current record's offset
                        trackOffset(partition, dupCandidate.metadata().offset());
                    }
                }
            }

        } finally {
            MDC.clear();
        }
    }

    private void processReleasedEvents(TopicPartition partition, List<SequencedEvent<T>> events) {
        for (SequencedEvent<T> event : events) {
            MDC.put(MDC_PIPELINE_STAGE, "DEDUPLICATION");
            DeduplicationResult dedupResult = deduplicationEngine.check(
                    partition, event.sourceEntity(), event.sequenceNumber());

            if (dedupResult == DeduplicationResult.DUPLICATE) {
                log.debug("Duplicate: entity={} seq={}", event.sourceEntity(), event.sequenceNumber());
                trackOffset(partition, event.metadata().offset());
                continue;
            }

            // Stage 5: Business handler invocation with retry
            MDC.put(MDC_PIPELINE_STAGE, "BUSINESS_LOGIC");
            invokeHandlerWithRetry(event, partition);
        }
    }

    private void invokeHandlerWithRetry(SequencedEvent<T> event, TopicPartition partition) {
        Instant startTime = Instant.now();
        int maxRetries = dlqProperties.maxRetryCount();
        Duration backoff = dlqProperties.initialBackoff();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                ProcessingResult result = eventHandler.handle(event.payload(), event.metadata());

                if (result == ProcessingResult.SUCCESS) {
                    Duration latency = Duration.between(startTime, Instant.now());
                    deduplicationEngine.markProcessed(partition, event.sourceEntity(), event.sequenceNumber());
                    trackOffset(partition, event.metadata().offset());
                    healthMonitor.recordSuccess(partition, latency);
                    metricsExporter.recordProcessingSuccess(partition);
                    metricsExporter.recordProcessingLatency(partition, latency);
                    healthMonitor.evaluate(partition);
                    return;
                }
                // If handler returns non-success, treat as permanent failure
                throw new PermanentProcessingException("Handler returned " + result);

            } catch (TransientProcessingException e) {
                if (attempt == maxRetries) {
                    log.warn("Transient failure exhausted retries for entity={} seq={}",
                            event.sourceEntity(), event.sequenceNumber());
                    Duration latency = Duration.between(startTime, Instant.now());
                    healthMonitor.recordFailure(partition, latency);
                    metricsExporter.recordProcessingFailure(partition);
                    healthMonitor.evaluate(partition);
                    routeToDlq(null, e, ErrorClassification.TRANSIENT, attempt + 1,
                            event.sourceEntity(), MDC.get(MDC_CORRELATION_ID));
                    trackOffset(partition, event.metadata().offset());
                    return;
                }

                // Non-blocking retry strategy:
                // - Immediate retries (backoff <= 50ms): sleep on poll thread (negligible impact)
                // - Longer backoffs: route to DLQ rather than blocking the poll thread.
                //   This preserves partition isolation — other partitions are never delayed
                //   by another partition's transient failures.
                // Future enhancement: scheduled retry queue that re-inserts events after delay.
                long sleepMs = Math.min(backoff.toMillis(), dlqProperties.maxBackoff().toMillis());
                if (sleepMs <= 50) {
                    try { Thread.sleep(sleepMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    // Backoff is too long to block poll thread — route to DLQ as transient
                    // rather than degrading isolation for other partitions.
                    log.info("Backoff {}ms too long for poll thread, routing to DLQ after {} attempts",
                            sleepMs, attempt + 1);
                    Duration latency = Duration.between(startTime, Instant.now());
                    healthMonitor.recordFailure(partition, latency);
                    metricsExporter.recordProcessingFailure(partition);
                    healthMonitor.evaluate(partition);
                    routeToDlq(null, e, ErrorClassification.TRANSIENT, attempt + 1,
                            event.sourceEntity(), MDC.get(MDC_CORRELATION_ID));
                    trackOffset(partition, event.metadata().offset());
                    return;
                }
                // Double backoff, cap at max
                backoff = Duration.ofMillis(Math.min(
                        backoff.toMillis() * 2, dlqProperties.maxBackoff().toMillis()));

            } catch (PermanentProcessingException e) {
                log.warn("Permanent failure for entity={} seq={}",
                        event.sourceEntity(), event.sequenceNumber());
                Duration latency = Duration.between(startTime, Instant.now());
                healthMonitor.recordFailure(partition, latency);
                metricsExporter.recordProcessingFailure(partition);
                healthMonitor.evaluate(partition);
                routeToDlq(null, e, ErrorClassification.PERMANENT, 0,
                        event.sourceEntity(), MDC.get(MDC_CORRELATION_ID));
                trackOffset(partition, event.metadata().offset());
                return;

            } catch (Exception e) {
                log.error("Unexpected error processing event", e);
                Duration latency = Duration.between(startTime, Instant.now());
                healthMonitor.recordFailure(partition, latency);
                metricsExporter.recordProcessingFailure(partition);
                healthMonitor.evaluate(partition);
                routeToDlq(null, e, ErrorClassification.PERMANENT, 0,
                        event.sourceEntity(), MDC.get(MDC_CORRELATION_ID));
                trackOffset(partition, event.metadata().offset());
                return;
            }
        }
    }

    // ========================= DLQ and Offset =========================

    private void routeToDlq(ConsumerRecord<String, byte[]> record, Throwable error,
                            ErrorClassification classification, int retryCount,
                            String sourceEntity, String correlationId) {
        if (record != null) {
            dlqRouter.route(record, error, classification, retryCount, sourceEntity, correlationId);
        } else {
            // Event came from reorder buffer (already deserialized) — construct enriched record
            // with full context in headers for DLQ consumers and replay tooling.
            org.apache.kafka.common.header.internals.RecordHeaders headers =
                    new org.apache.kafka.common.header.internals.RecordHeaders();
            if (sourceEntity != null) {
                headers.add("dlq.source.entity", sourceEntity.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            if (correlationId != null) {
                headers.add("dlq.correlation.id", correlationId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            headers.add("dlq.error.classification", classification.name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            headers.add("dlq.retry.count", String.valueOf(retryCount).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            headers.add("dlq.error.message",
                    (error.getMessage() != null ? error.getMessage() : error.getClass().getName())
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            headers.add("dlq.origin", "reorder-buffer".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            ConsumerRecord<String, byte[]> syntheticRecord = new ConsumerRecord<>(
                    "dlq.synthetic", 0, -1,
                    ConsumerRecord.NO_TIMESTAMP, org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE,
                    -1, -1,
                    sourceEntity != null ? sourceEntity : "unknown",
                    error.getMessage() != null ? error.getMessage().getBytes() : new byte[0],
                    headers,
                    Optional.empty()
            );
            dlqRouter.route(syntheticRecord, error, classification, retryCount, sourceEntity, correlationId);
        }
        log.info("Routed to DLQ: classification={}, entity={}, retries={}",
                classification, sourceEntity, retryCount);
    }

    /**
     * Tracks the highest processed offset per-partition for batched commit.
     * Only the highest offset per partition is retained (monotonically increasing).
     */
    private void trackOffset(TopicPartition partition, long offset) {
        pendingOffsets.merge(partition, new OffsetAndMetadata(offset + 1),
                (existing, incoming) -> incoming.offset() > existing.offset() ? incoming : existing);
    }

    /**
     * Commits all accumulated offsets in a single synchronous batch at end of poll cycle.
     * This is called once per poll() iteration instead of per-record, dramatically
     * reducing commit overhead from O(records) to O(1) per poll batch.
     */
    private void flushPendingOffsets() {
        if (pendingOffsets.isEmpty()) return;
        try {
            consumer.commitSync(new HashMap<>(pendingOffsets));
            pendingOffsets.clear();
        } catch (Exception e) {
            log.error("Failed to commit batched offsets", e);
            // Offsets remain in pendingOffsets for next flush attempt
        }
    }

    /**
     * Legacy per-record synchronous commit — only used during rebalance/shutdown
     * where we need immediate durability guarantees.
     */
    private void commitOffset(TopicPartition partition, long offset) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, new OffsetAndMetadata(offset + 1));
        consumer.commitSync(offsets);
    }

    // ========================= Pause/Resume =========================

    /**
     * Pauses consumption for a partition. Thread-safe: if called from a non-poll thread,
     * the action is enqueued and executed on the poll thread at the next iteration.
     */
    public void pausePartition(TopicPartition partition) {
        if (Thread.currentThread() == pollThread) {
            consumer.pause(Collections.singleton(partition));
            log.info("Paused partition {}", partition);
        } else {
            consumerActions.add(() -> {
                consumer.pause(Collections.singleton(partition));
                log.info("Paused partition {} (via command queue)", partition);
            });
        }
    }

    /**
     * Resumes consumption for a partition. Thread-safe: if called from a non-poll thread,
     * the action is enqueued and executed on the poll thread at the next iteration.
     */
    public void resumePartition(TopicPartition partition) {
        if (Thread.currentThread() == pollThread) {
            consumer.resume(Collections.singleton(partition));
            log.info("Resumed partition {}", partition);
        } else {
            consumerActions.add(() -> {
                consumer.resume(Collections.singleton(partition));
                log.info("Resumed partition {} (via command queue)", partition);
            });
        }
    }

    // ========================= ConsumerRebalanceListener =========================

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);

        // Update assignment snapshot immediately (this callback runs on the poll thread)
        Set<TopicPartition> remaining = new java.util.HashSet<>(assignedPartitionsSnapshot.get());
        remaining.removeAll(partitions);
        assignedPartitionsSnapshot.set(Set.copyOf(remaining));

        // Flush any pending batched offsets before revocation
        flushPendingOffsets();

        // Commit offsets for revoked partitions
        try {
            consumer.commitSync(properties.rebalanceCommitTimeout());
        } catch (Exception e) {
            log.error("Failed to commit offsets during revocation", e);
        }

        // Persist state for revoked partitions
        for (TopicPartition partition : partitions) {
            try {
                Map<TopicPartition, CircuitBreakerSnapshot> cbSnapshots = circuitBreaker.snapshot();
                stateStore.persistCircuitBreakerState(
                        Map.of(partition, cbSnapshots.getOrDefault(partition,
                                new CircuitBreakerSnapshot(CircuitState.CLOSED, Instant.now(), 0, Instant.now()))));

                Map<TopicPartition, BufferSnapshot<T>> bufferSnapshots = reorderBuffer.snapshot();
                if (bufferSnapshots.containsKey(partition)) {
                    stateStore.persistBufferState(Map.of(partition, bufferSnapshots.get(partition)));
                }

                DeduplicationSnapshot dedupSnapshot = deduplicationEngine.snapshot(partition);
                stateStore.persistDeduplicationState(Map.of(partition, dedupSnapshot));

            } catch (Exception e) {
                log.error("Failed to persist state for partition {}", partition, e);
            }

            // Cleanup components
            healthMonitor.resetPartition(partition);
            circuitBreaker.removePartition(partition);
            reorderBuffer.removePartition(partition);
            metricsExporter.unregisterPartition(partition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partitions assigned: {}", partitions);

        // Update assignment snapshot immediately (this callback runs on the poll thread)
        Set<TopicPartition> updated = new java.util.HashSet<>(assignedPartitionsSnapshot.get());
        updated.addAll(partitions);
        assignedPartitionsSnapshot.set(Set.copyOf(updated));

        for (TopicPartition partition : partitions) {
            try {
                // Restore state from persistent storage
                Optional<CircuitBreakerSnapshot> cbSnapshot = stateStore.restoreCircuitBreakerState(partition);
                if (cbSnapshot.isPresent()) {
                    circuitBreaker.restore(Map.of(partition, cbSnapshot.get()));
                } else {
                    circuitBreaker.initializePartition(partition);
                }
            } catch (Exception e) {
                log.warn("Failed to restore circuit breaker state for partition {}, initializing fresh", partition, e);
                circuitBreaker.initializePartition(partition);
            }

            try {
                Optional<BufferSnapshot<T>> bufferSnapshot =
                        (Optional<BufferSnapshot<T>>) (Optional<?>) stateStore.restoreBufferState(partition);
                if (bufferSnapshot.isPresent()) {
                    reorderBuffer.restore(Map.of(partition, bufferSnapshot.get()));
                } else {
                    reorderBuffer.initializePartition(partition);
                }
            } catch (Exception e) {
                log.warn("Failed to restore buffer state for partition {}, initializing fresh", partition, e);
                reorderBuffer.initializePartition(partition);
            }

            try {
                Optional<DeduplicationSnapshot> dedupSnapshot = stateStore.restoreDeduplicationState(partition);
                if (dedupSnapshot.isPresent()) {
                    deduplicationEngine.restore(partition, dedupSnapshot.get());
                }
            } catch (Exception e) {
                log.warn("Failed to restore deduplication state for partition {}, continuing without it", partition, e);
            }

            healthMonitor.initializePartition(partition);
            metricsExporter.registerPartition(partition);
        }
    }

    // ========================= Shutdown =========================

    private void performGracefulShutdown() {
        try {
            // Flush reorder buffers
            for (TopicPartition partition : consumer.assignment()) {
                List<SequencedEvent<T>> flushed = reorderBuffer.flushPartition(partition);
                if (!flushed.isEmpty()) {
                    processReleasedEvents(partition, flushed);
                }
            }

            // Flush any pending batched offsets accumulated during buffer flush processing
            flushPendingOffsets();

            // Commit final offsets (safety net — flushPendingOffsets should have covered it)
            consumer.commitSync(properties.shutdownTimeout());

            // Persist final state
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, CircuitBreakerSnapshot> cbSnapshots = circuitBreaker.snapshot();
            if (!cbSnapshots.isEmpty()) {
                stateStore.persistCircuitBreakerState(cbSnapshots);
            }

        } catch (Exception e) {
            log.error("Error during graceful shutdown", e);
        } finally {
            consumer.close(Duration.ofSeconds(5));
        }
    }

    private void forceShutdown() {
        try {
            // Try to persist what we can
            Map<TopicPartition, CircuitBreakerSnapshot> cbSnapshots = circuitBreaker.snapshot();
            if (!cbSnapshots.isEmpty()) {
                stateStore.persistCircuitBreakerState(cbSnapshots);
            }

            // Log uncommitted offset ranges
            for (TopicPartition partition : consumer.assignment()) {
                log.warn("Force shutdown: partition {} may have uncommitted offsets", partition);
            }
        } catch (Exception e) {
            log.error("Error during force shutdown", e);
        } finally {
            consumer.close(Duration.ZERO);
        }
    }

    // ========================= State Restoration =========================

    private void restoreState() {
        Set<TopicPartition> assigned = consumer.assignment();
        if (assigned.isEmpty()) return;

        log.info("Restoring state for {} partitions", assigned.size());
        for (TopicPartition partition : assigned) {
            try {
                Optional<CircuitBreakerSnapshot> cbSnapshot = stateStore.restoreCircuitBreakerState(partition);
                if (cbSnapshot.isPresent()) {
                    circuitBreaker.restore(Map.of(partition, cbSnapshot.get()));
                    log.debug("Restored circuit breaker state for {}", partition);
                }

                Optional<BufferSnapshot<T>> bufferSnapshot =
                        (Optional<BufferSnapshot<T>>) (Optional<?>) stateStore.restoreBufferState(partition);
                if (bufferSnapshot.isPresent()) {
                    reorderBuffer.restore(Map.of(partition, bufferSnapshot.get()));
                    log.debug("Restored reorder buffer state for {}", partition);
                }

                Optional<DeduplicationSnapshot> dedupSnapshot = stateStore.restoreDeduplicationState(partition);
                if (dedupSnapshot.isPresent()) {
                    deduplicationEngine.restore(partition, dedupSnapshot.get());
                    log.debug("Restored dedup state for {}", partition);
                }
            } catch (Exception e) {
                log.error("Failed to restore state for partition {}, starting fresh", partition, e);
            }
        }
    }

    // ========================= Subscription =========================

    /**
     * Subscribes to the given topics and starts consuming.
     * Must be called before start() if not using auto-subscription.
     */
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics, this);
    }
}