package com.framework.resilient.reorder;

import com.framework.resilient.coordinator.EventMetadata;

import java.time.Instant;

/**
 * An event with sequence tracking information for reorder buffering.
 *
 * @param sourceEntity   the logical producer of sequenced events (e.g., account, device)
 * @param sequenceNumber monotonically increasing identifier per source entity
 * @param payload        the deserialized event payload
 * @param metadata       processing context (topic, partition, offset, headers, timestamps)
 * @param bufferedAt     when the event was placed into the reorder buffer (null if not buffered)
 * @param <T>            the deserialized event payload type
 */
public record SequencedEvent<T>(
        String sourceEntity,
        long sequenceNumber,
        T payload,
        EventMetadata metadata,
        Instant bufferedAt
) {
}
