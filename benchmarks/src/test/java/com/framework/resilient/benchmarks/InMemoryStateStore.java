package com.framework.resilient.benchmarks;

import com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot;
import com.framework.resilient.coordinator.StateStore;
import com.framework.resilient.dedup.DeduplicationSnapshot;
import com.framework.resilient.reorder.BufferSnapshot;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory StateStore implementation for benchmark tests.
 * Provides fast, no-IO state persistence suitable for isolated benchmark execution.
 */
public class InMemoryStateStore implements StateStore {

    private final ConcurrentHashMap<TopicPartition, CircuitBreakerSnapshot> cbState = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, BufferSnapshot<?>> bufferState = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, DeduplicationSnapshot> dedupState = new ConcurrentHashMap<>();

    @Override
    public void persistCircuitBreakerState(Map<TopicPartition, CircuitBreakerSnapshot> snapshots) {
        cbState.putAll(snapshots);
    }

    @Override
    public Optional<CircuitBreakerSnapshot> restoreCircuitBreakerState(TopicPartition partition) {
        return Optional.ofNullable(cbState.get(partition));
    }

    @Override
    public <T> void persistBufferState(Map<TopicPartition, BufferSnapshot<T>> snapshots) {
        bufferState.putAll(snapshots);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<BufferSnapshot<T>> restoreBufferState(TopicPartition partition) {
        return Optional.ofNullable((BufferSnapshot<T>) bufferState.get(partition));
    }

    @Override
    public void persistDeduplicationState(Map<TopicPartition, DeduplicationSnapshot> snapshots) {
        dedupState.putAll(snapshots);
    }

    @Override
    public Optional<DeduplicationSnapshot> restoreDeduplicationState(TopicPartition partition) {
        return Optional.ofNullable(dedupState.get(partition));
    }

    @Override
    public void removeState(TopicPartition partition) {
        cbState.remove(partition);
        bufferState.remove(partition);
        dedupState.remove(partition);
    }

    @Override
    public boolean isAvailable() {
        return true;
    }
}
