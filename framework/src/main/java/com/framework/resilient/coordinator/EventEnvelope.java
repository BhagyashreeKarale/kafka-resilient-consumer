package com.framework.resilient.coordinator;

/**
 * Wraps a raw Kafka record with framework metadata.
 * The payload type T is determined by the application's EventHandler.
 *
 * @param sourceEntity   the logical producer of sequenced events (e.g., account, device, session)
 * @param sequenceNumber monotonically increasing identifier per source entity
 * @param payload        the deserialized event payload
 * @param metadata       processing context (topic, partition, offset, headers, timestamps)
 * @param <T>            the deserialized event payload type
 */
public record EventEnvelope<T>(
        String sourceEntity,
        long sequenceNumber,
        T payload,
        EventMetadata metadata
) {
}
