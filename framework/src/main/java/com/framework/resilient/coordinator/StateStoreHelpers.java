package com.framework.resilient.coordinator;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

/**
 * Typed helpers for StateStore to avoid unsafe casts at call sites.
 * These are small, non-invasive additions that delegate to the existing generic methods
 * but provide a clearer typed API for callers like ConsumerCoordinator.
 */
public final class StateStoreHelpers {

    private StateStoreHelpers() {}

    public static <T> Optional<BufferSnapshot<T>> restoreTypedBufferState(StateStore store, TopicPartition partition) {
        return store.restoreBufferState(partition);
    }
}
