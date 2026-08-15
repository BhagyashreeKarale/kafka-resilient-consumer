package com.framework.resilient.reorder;

import java.util.List;
import java.util.Map;

/**
 * Snapshot of a partition's reorder buffer state for persistence during rebalance.
 * Contains the buffered events and per-entity sequence tracking state.
 *
 * @param bufferedEvents        list of events currently held in the buffer
 * @param expectedNextSequences map of source entity to expected next sequence number
 * @param <T>                   the deserialized event payload type
 */
public record BufferSnapshot<T>(
        List<SequencedEvent<T>> bufferedEvents,
        Map<String, Long> expectedNextSequences
) {
}
