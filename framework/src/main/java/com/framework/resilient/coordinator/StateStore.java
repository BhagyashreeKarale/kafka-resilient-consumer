package com.framework.resilient.coordinator;

import com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot;
import com.framework.resilient.dedup.DeduplicationSnapshot;
import com.framework.resilient.reorder.BufferSnapshot;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;

/**
 * Pluggable persistence interface for partition state.
 * Used during rebalance (persist on revoke, restore on assign) and shutdown/startup.
 */
public interface StateStore {

    /**
     * Persist circuit breaker state for the given partitions.
     */
    void persistCircuitBreakerState(Map<TopicPartition, CircuitBreakerSnapshot> snapshots);

    /**
     * Restore circuit breaker state for the given partition.
     */
    Optional<CircuitBreakerSnapshot> restoreCircuitBreakerState(TopicPartition partition);

    /**
     * Persist reorder buffer state for the given partitions.
     */
    <T> void persistBufferState(Map<TopicPartition, BufferSnapshot<T>> snapshots);

    /**
     * Restore reorder buffer state for the given partition.
     */
    <T> Optional<BufferSnapshot<T>> restoreBufferState(TopicPartition partition);

    /**
     * Persist deduplication state for the given partitions.
     */
    void persistDeduplicationState(Map<TopicPartition, DeduplicationSnapshot> snapshots);

    /**
     * Restore deduplication state for the given partition.
     */
    Optional<DeduplicationSnapshot> restoreDeduplicationState(TopicPartition partition);

    /**
     * Remove all persisted state for the given partition.
     */
    void removeState(TopicPartition partition);

    /**
     * Check if the state store is available and healthy.
     */
    boolean isAvailable();
}
