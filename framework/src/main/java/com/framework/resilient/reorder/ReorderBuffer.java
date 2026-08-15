package com.framework.resilient.reorder;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Per-Source_Entity sequence tracking with time-bounded buffering and backpressure.
 * <p>
 * Maintains independent sequence tracking per Source_Entity within each partition.
 * Buffers out-of-order events until predecessors arrive or timeout triggers forced release.
 * Applies backpressure via pause() when buffer reaches max capacity, resumes at 80%.
 * </p>
 * <p>
 * This class is designed to be called from a single-threaded poll loop.
 * The ConcurrentHashMap is used for safe reads from metrics/monitoring threads.
 * </p>
 *
 * @param <T> the deserialized event payload type
 */
public class ReorderBuffer<T> {

    private static final Logger log = LoggerFactory.getLogger(ReorderBuffer.class);

    private final ConcurrentHashMap<TopicPartition, PartitionBuffer<T>> partitionBuffers;
    private final ReorderBufferProperties properties;
    private final Consumer<TopicPartition> pauseCallback;
    private final Consumer<TopicPartition> resumeCallback;

    /**
     * Creates a new ReorderBuffer with the given configuration and pause/resume callbacks.
     *
     * @param properties      buffer configuration (max size, resume threshold, reorder timeout)
     * @param pauseCallback   invoked when backpressure requires pausing a partition
     * @param resumeCallback  invoked when backpressure is lifted and partition can resume
     */
    public ReorderBuffer(ReorderBufferProperties properties,
                         Consumer<TopicPartition> pauseCallback,
                         Consumer<TopicPartition> resumeCallback) {
        this.partitionBuffers = new ConcurrentHashMap<>();
        this.properties = properties;
        this.pauseCallback = pauseCallback;
        this.resumeCallback = resumeCallback;
    }

    // --- Event submission ---

    /**
     * Submits an event to the reorder buffer for the given partition.
     * <p>
     * Logic:
     * <ol>
     *   <li>If first event for unseen Source_Entity → accept immediately, set expectedNext = seq + 1</li>
     *   <li>If seq == expectedNext → release it + any consecutive buffered events</li>
     *   <li>If seq > expectedNext → buffer in TreeMap, return empty released list</li>
     *   <li>If seq &lt;= lastProcessed → forward to dedup (returned in possibleDuplicates)</li>
     * </ol>
     * </p>
     *
     * @param partition the topic partition the event belongs to
     * @param event     the sequenced event to submit
     * @return a {@link ReorderResult} containing released events, possible duplicates, and backpressure state
     */
    public ReorderResult<T> submit(TopicPartition partition, SequencedEvent<T> event) {
        PartitionBuffer<T> buffer = partitionBuffers.computeIfAbsent(partition, k -> new PartitionBuffer<>());

        String sourceEntity = event.sourceEntity();
        long seq = event.sequenceNumber();

        PartitionBuffer.EntitySequenceState<T> entityState = buffer.getOrCreateEntityState(sourceEntity);

        // First event for unseen entity: accept immediately, set expected next
        if (!entityState.isInitialized()) {
            entityState.initialize(seq);
            List<SequencedEvent<T>> released = new ArrayList<>();
            released.add(event);
            // Also check if there are buffered events that follow consecutively
            released.addAll(entityState.releaseConsecutive());
            return new ReorderResult<>(released, List.of(), false);
        }

        // Event with seq <= last processed: forward to dedup
        if (seq <= entityState.getLastProcessedSequence()) {
            return ReorderResult.duplicates(List.of(event));
        }

        // In-order event: release immediately along with any consecutive buffered events
        if (seq == entityState.getExpectedNextSequence()) {
            List<SequencedEvent<T>> released = new ArrayList<>();
            released.add(event);
            entityState.advanceExpectedNext();
            // Release any consecutive buffered events
            released.addAll(entityState.releaseConsecutive());

            // Decrement total count for each released buffered event
            int releasedBuffered = released.size() - 1; // first event wasn't buffered
            for (int i = 0; i < releasedBuffered; i++) {
                buffer.decrementTotalCount();
            }

            // Check if backpressure can be lifted
            checkResumeBackpressure(partition, buffer);

            return new ReorderResult<>(released, List.of(), false);
        }

        // Out-of-order event (seq > expectedNext): buffer it
        SequencedEvent<T> bufferedEvent = new SequencedEvent<>(
                event.sourceEntity(),
                event.sequenceNumber(),
                event.payload(),
                event.metadata(),
                event.bufferedAt() != null ? event.bufferedAt() : Instant.now()
        );
        entityState.bufferEvent(bufferedEvent);
        buffer.incrementTotalCount();

        // Check if backpressure should be triggered
        boolean backpressureTriggered = checkTriggerBackpressure(partition, buffer);

        return new ReorderResult<>(List.of(), List.of(), backpressureTriggered);
    }

    // --- Timeout processing ---

    /**
     * Releases events that have exceeded the configured reorder timeout for the given partition.
     * <p>
     * Iterates all entities in the partition, finds events where the time since buffering
     * exceeds {@code reorderTimeout}, releases them in sequence order, and logs a gap warning
     * identifying the Source_Entity, partition, and missing Sequence_Number range.
     * Updates expectedNextSequence to the released event's sequence number plus one.
     * </p>
     *
     * @param partition the topic partition to check for timed-out events
     * @return list of released events in sequence order
     */
    public List<SequencedEvent<T>> releaseTimedOut(TopicPartition partition) {
        PartitionBuffer<T> buffer = partitionBuffers.get(partition);
        if (buffer == null) {
            return List.of();
        }

        Duration timeout = properties.reorderTimeout();
        Instant cutoff = Instant.now().minus(timeout);
        List<SequencedEvent<T>> released = new ArrayList<>();

        for (Map.Entry<String, PartitionBuffer.EntitySequenceState<T>> entry : buffer.getEntityStates().entrySet()) {
            String sourceEntity = entry.getKey();
            PartitionBuffer.EntitySequenceState<T> entityState = entry.getValue();

            List<SequencedEvent<T>> timedOut = entityState.releaseTimedOut(cutoff);
            for (SequencedEvent<T> event : timedOut) {
                long expectedNext = entityState.getExpectedNextSequence();
                // The gap is from the current expected to the released event's sequence
                long gapStart = expectedNext;
                long gapEnd = event.sequenceNumber() - 1;

                if (gapStart <= gapEnd) {
                    log.warn("Releasing timed-out event with gap. sourceEntity={}, partition={}, "
                                    + "missingSequenceRange=[{}, {}], releasedSequence={}",
                            sourceEntity, partition, gapStart, gapEnd, event.sequenceNumber());
                }

                // Update expected next to seq + 1 after releasing
                entityState.setExpectedNextSequence(event.sequenceNumber() + 1);
                buffer.decrementTotalCount();
                released.add(event);

                // Also release any consecutive events that follow
                List<SequencedEvent<T>> consecutive = entityState.releaseConsecutive();
                for (SequencedEvent<T> consec : consecutive) {
                    buffer.decrementTotalCount();
                }
                released.addAll(consecutive);
            }
        }

        // Check if backpressure can be lifted
        if (!released.isEmpty()) {
            checkResumeBackpressure(partition, buffer);
        }

        return released;
    }

    // --- Buffer queries ---

    /**
     * Returns the total number of buffered events for a partition.
     *
     * @param partition the topic partition to query
     * @return the current buffer depth, or 0 if the partition is not tracked
     */
    public int getBufferDepth(TopicPartition partition) {
        PartitionBuffer<T> buffer = partitionBuffers.get(partition);
        return buffer != null ? buffer.getTotalBufferedCount() : 0;
    }

    /**
     * Returns whether backpressure is currently active for a partition.
     *
     * @param partition the topic partition to query
     * @return true if backpressure is active (partition paused due to buffer capacity)
     */
    public boolean isBackpressureActive(TopicPartition partition) {
        PartitionBuffer<T> buffer = partitionBuffers.get(partition);
        return buffer != null && buffer.isBackpressureActive();
    }

    // --- Lifecycle ---

    /**
     * Initializes tracking for a newly assigned partition.
     * Creates an empty partition buffer if one does not already exist.
     *
     * @param partition the topic partition to initialize
     */
    public void initializePartition(TopicPartition partition) {
        partitionBuffers.putIfAbsent(partition, new PartitionBuffer<>());
    }

    /**
     * Flushes all buffered events for a partition, releasing them in sequence order.
     * Lifts any active backpressure on the partition.
     *
     * @param partition the topic partition to flush
     * @return all buffered events in ascending sequence order
     */
    public List<SequencedEvent<T>> flushPartition(TopicPartition partition) {
        PartitionBuffer<T> buffer = partitionBuffers.get(partition);
        if (buffer == null) {
            return List.of();
        }

        List<SequencedEvent<T>> released = new ArrayList<>();

        for (Map.Entry<String, PartitionBuffer.EntitySequenceState<T>> entry : buffer.getEntityStates().entrySet()) {
            PartitionBuffer.EntitySequenceState<T> entityState = entry.getValue();
            released.addAll(entityState.flushAll());
        }

        buffer.resetTotalCount();

        // Lift backpressure if it was active
        if (buffer.isBackpressureActive()) {
            buffer.setBackpressureActive(false);
            resumeCallback.accept(partition);
        }

        return released;
    }

    /**
     * Removes all state for a partition (used on revocation after snapshot).
     *
     * @param partition the topic partition to remove
     */
    public void removePartition(TopicPartition partition) {
        partitionBuffers.remove(partition);
    }

    // --- State persistence ---

    /**
     * Creates a snapshot of all partition buffer states for persistence during rebalance.
     *
     * @return map of partition to buffer snapshot containing buffered events and sequence tracking state
     */
    public Map<TopicPartition, BufferSnapshot<T>> snapshot() {
        Map<TopicPartition, BufferSnapshot<T>> snapshots = new HashMap<>();

        for (Map.Entry<TopicPartition, PartitionBuffer<T>> entry : partitionBuffers.entrySet()) {
            TopicPartition partition = entry.getKey();
            PartitionBuffer<T> buffer = entry.getValue();

            List<SequencedEvent<T>> bufferedEvents = new ArrayList<>();
            Map<String, Long> expectedNextSequences = new HashMap<>();

            for (Map.Entry<String, PartitionBuffer.EntitySequenceState<T>> entityEntry : buffer.getEntityStates().entrySet()) {
                String sourceEntity = entityEntry.getKey();
                PartitionBuffer.EntitySequenceState<T> state = entityEntry.getValue();

                expectedNextSequences.put(sourceEntity, state.getExpectedNextSequence());
                bufferedEvents.addAll(state.getBufferedEvents());
            }

            snapshots.put(partition, new BufferSnapshot<>(bufferedEvents, expectedNextSequences));
        }

        return snapshots;
    }

    /**
     * Restores partition buffer states from previously persisted snapshots.
     * Replaces any existing state for the given partitions.
     *
     * @param snapshots map of partition to buffer snapshot to restore
     */
    public void restore(Map<TopicPartition, BufferSnapshot<T>> snapshots) {
        for (Map.Entry<TopicPartition, BufferSnapshot<T>> entry : snapshots.entrySet()) {
            TopicPartition partition = entry.getKey();
            BufferSnapshot<T> snapshot = entry.getValue();

            PartitionBuffer<T> buffer = new PartitionBuffer<>();

            // Restore expected next sequences
            for (Map.Entry<String, Long> seqEntry : snapshot.expectedNextSequences().entrySet()) {
                String sourceEntity = seqEntry.getKey();
                long expectedNext = seqEntry.getValue();

                PartitionBuffer.EntitySequenceState<T> entityState = buffer.getOrCreateEntityState(sourceEntity);
                entityState.restoreState(expectedNext);
            }

            // Restore buffered events
            for (SequencedEvent<T> event : snapshot.bufferedEvents()) {
                PartitionBuffer.EntitySequenceState<T> entityState = buffer.getOrCreateEntityState(event.sourceEntity());
                entityState.bufferEvent(event);
                buffer.incrementTotalCount();
            }

            partitionBuffers.put(partition, buffer);
        }
    }

    // --- Backpressure logic ---

    private boolean checkTriggerBackpressure(TopicPartition partition, PartitionBuffer<T> buffer) {
        if (!buffer.isBackpressureActive() && buffer.getTotalBufferedCount() >= properties.maxBufferSize()) {
            buffer.setBackpressureActive(true);
            pauseCallback.accept(partition);
            log.warn("Backpressure triggered for partition {}. Buffer size: {}, max: {}",
                    partition, buffer.getTotalBufferedCount(), properties.maxBufferSize());
            return true;
        }
        return false;
    }

    private void checkResumeBackpressure(TopicPartition partition, PartitionBuffer<T> buffer) {
        if (buffer.isBackpressureActive()) {
            int resumeThreshold = (int) (properties.maxBufferSize() * properties.resumeThreshold());
            if (buffer.getTotalBufferedCount() < resumeThreshold) {
                buffer.setBackpressureActive(false);
                resumeCallback.accept(partition);
                log.info("Backpressure released for partition {}. Buffer size: {}, resume threshold: {}",
                        partition, buffer.getTotalBufferedCount(), resumeThreshold);
            }
        }
    }
}
