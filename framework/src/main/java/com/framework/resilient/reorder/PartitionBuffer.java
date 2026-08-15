package com.framework.resilient.reorder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Per-partition buffer tracking all entity sequence states and backpressure.
 * <p>
 * Tracks per-Source_Entity state within a single partition using a map of
 * {@link EntitySequenceState} keyed by source entity identifier. Manages
 * backpressure state and total buffered event count across all entities.
 * </p>
 * <p>
 * This class is package-private — used only by {@link ReorderBuffer}.
 * </p>
 *
 * @param <T> the deserialized event payload type
 */
class PartitionBuffer<T> {

    private final Map<String, EntitySequenceState<T>> entityStates;
    private int totalBufferedCount;
    private boolean backpressureActive;

    PartitionBuffer() {
        this.entityStates = new HashMap<>();
        this.totalBufferedCount = 0;
        this.backpressureActive = false;
    }

    /**
     * Gets or creates the entity sequence state for the given source entity.
     */
    EntitySequenceState<T> getOrCreateEntityState(String sourceEntity) {
        return entityStates.computeIfAbsent(sourceEntity, k -> new EntitySequenceState<>());
    }

    /**
     * Returns the map of all entity states (keyed by source entity identifier).
     */
    Map<String, EntitySequenceState<T>> getEntityStates() {
        return entityStates;
    }

    /**
     * Returns the total number of buffered events across all entities in this partition.
     */
    int getTotalBufferedCount() {
        return totalBufferedCount;
    }

    /**
     * Increments the total buffered event count by one.
     */
    void incrementTotalCount() {
        totalBufferedCount++;
    }

    /**
     * Decrements the total buffered event count by one (minimum 0).
     */
    void decrementTotalCount() {
        if (totalBufferedCount > 0) {
            totalBufferedCount--;
        }
    }

    /**
     * Resets the total buffered event count to zero.
     */
    void resetTotalCount() {
        totalBufferedCount = 0;
    }

    /**
     * Returns whether backpressure is currently active for this partition.
     */
    boolean isBackpressureActive() {
        return backpressureActive;
    }

    /**
     * Sets the backpressure state for this partition.
     */
    void setBackpressureActive(boolean active) {
        this.backpressureActive = active;
    }

    /**
     * Per-Source_Entity sequence tracking state within a partition.
     * <p>
     * Tracks expected next sequence number, last processed sequence number,
     * and maintains a {@link TreeMap} of buffered out-of-order events keyed
     * by sequence number for efficient ordered retrieval.
     * </p>
     *
     * @param <T> the deserialized event payload type
     */
    static class EntitySequenceState<T> {

        private long expectedNextSequence;
        private long lastProcessedSequence;
        private boolean initialized;
        private final TreeMap<Long, SequencedEvent<T>> bufferedEvents;

        EntitySequenceState() {
            this.expectedNextSequence = -1;
            this.lastProcessedSequence = -1;
            this.initialized = false;
            this.bufferedEvents = new TreeMap<>();
        }

        /**
         * Returns whether this entity state has been initialized with a first event.
         */
        boolean isInitialized() {
            return initialized;
        }

        /**
         * Initialize with the first seen sequence number.
         * Sets expected next to seq + 1 and last processed to seq.
         */
        void initialize(long firstSequence) {
            this.initialized = true;
            this.expectedNextSequence = firstSequence + 1;
            this.lastProcessedSequence = firstSequence;
        }

        /**
         * Returns the expected next sequence number for this entity.
         */
        long getExpectedNextSequence() {
            return expectedNextSequence;
        }

        /**
         * Sets the expected next sequence number and updates last processed accordingly.
         */
        void setExpectedNextSequence(long seq) {
            this.expectedNextSequence = seq;
            this.lastProcessedSequence = seq - 1;
        }

        /**
         * Returns the last processed sequence number for this entity.
         */
        long getLastProcessedSequence() {
            return lastProcessedSequence;
        }

        /**
         * Advances expected next sequence by one and updates last processed.
         */
        void advanceExpectedNext() {
            this.lastProcessedSequence = this.expectedNextSequence;
            this.expectedNextSequence++;
        }

        /**
         * Buffers an out-of-order event keyed by its sequence number.
         */
        void bufferEvent(SequencedEvent<T> event) {
            bufferedEvents.put(event.sequenceNumber(), event);
        }

        /**
         * Releases all consecutive events starting from expectedNextSequence.
         * Updates expectedNextSequence and lastProcessedSequence accordingly.
         *
         * @return list of released events in ascending sequence order
         */
        List<SequencedEvent<T>> releaseConsecutive() {
            List<SequencedEvent<T>> released = new ArrayList<>();

            while (!bufferedEvents.isEmpty()) {
                Map.Entry<Long, SequencedEvent<T>> first = bufferedEvents.firstEntry();
                if (first.getKey() == expectedNextSequence) {
                    released.add(first.getValue());
                    bufferedEvents.pollFirstEntry();
                    lastProcessedSequence = expectedNextSequence;
                    expectedNextSequence++;
                } else {
                    break;
                }
            }

            return released;
        }

        /**
         * Releases the earliest buffered event that has exceeded the timeout cutoff.
         * Only releases one event at a time from the front of the buffer to allow
         * the caller to update state and check for consecutive releases.
         *
         * @param cutoff events buffered before this instant are timed out
         * @return list of timed-out events (at most one per call to allow gap tracking)
         */
        List<SequencedEvent<T>> releaseTimedOut(Instant cutoff) {
            List<SequencedEvent<T>> released = new ArrayList<>();

            if (!bufferedEvents.isEmpty()) {
                Map.Entry<Long, SequencedEvent<T>> first = bufferedEvents.firstEntry();
                SequencedEvent<T> event = first.getValue();
                if (event.bufferedAt() != null && event.bufferedAt().isBefore(cutoff)) {
                    released.add(event);
                    bufferedEvents.pollFirstEntry();
                }
            }

            return released;
        }

        /**
         * Returns all buffered events in sequence order (for snapshot persistence).
         */
        List<SequencedEvent<T>> getBufferedEvents() {
            return new ArrayList<>(bufferedEvents.values());
        }

        /**
         * Flushes all buffered events in ascending sequence order.
         * Updates expected next and last processed to reflect the flushed state.
         *
         * @return all buffered events in sequence order
         */
        List<SequencedEvent<T>> flushAll() {
            List<SequencedEvent<T>> released = new ArrayList<>(bufferedEvents.values());
            if (!released.isEmpty()) {
                long lastSeq = bufferedEvents.lastKey();
                expectedNextSequence = lastSeq + 1;
                lastProcessedSequence = lastSeq;
            }
            bufferedEvents.clear();
            return released;
        }

        /**
         * Restores state from a snapshot (expected next sequence).
         */
        void restoreState(long expectedNext) {
            this.initialized = true;
            this.expectedNextSequence = expectedNext;
            this.lastProcessedSequence = expectedNext - 1;
        }
    }
}
