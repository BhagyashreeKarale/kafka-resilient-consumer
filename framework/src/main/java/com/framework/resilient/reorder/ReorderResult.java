package com.framework.resilient.reorder;

import java.util.List;

/**
 * Result of submitting an event to the reorder buffer.
 *
 * @param releasedEvents       events ready for downstream processing (in sequence order)
 * @param possibleDuplicates   events with seq <= last processed (forward to dedup)
 * @param backpressureTriggered whether max capacity was hit, causing partition pause
 * @param <T>                  the deserialized event payload type
 */
public record ReorderResult<T>(
        List<SequencedEvent<T>> releasedEvents,
        List<SequencedEvent<T>> possibleDuplicates,
        boolean backpressureTriggered
) {

    /**
     * Creates an empty result with no released events or duplicates.
     */
    public static <T> ReorderResult<T> empty() {
        return new ReorderResult<>(List.of(), List.of(), false);
    }

    /**
     * Creates a result with only released events.
     */
    public static <T> ReorderResult<T> released(List<SequencedEvent<T>> events) {
        return new ReorderResult<>(events, List.of(), false);
    }

    /**
     * Creates a result indicating possible duplicates forwarded for dedup.
     */
    public static <T> ReorderResult<T> duplicates(List<SequencedEvent<T>> events) {
        return new ReorderResult<>(List.of(), events, false);
    }
}
