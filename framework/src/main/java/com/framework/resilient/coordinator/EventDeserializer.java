package com.framework.resilient.coordinator;

import com.framework.resilient.dlq.DeserializationException;
import com.framework.resilient.reorder.SequencedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Functional interface for deserializing raw Kafka records into sequenced events.
 * Implementations convert the byte[] payload into the application's event type T
 * and extract source entity and sequence number for ordering.
 *
 * @param <T> the deserialized event payload type
 */
@FunctionalInterface
public interface EventDeserializer<T> {

    /**
     * Deserializes a raw Kafka consumer record into a sequenced event.
     *
     * @param record the raw Kafka consumer record
     * @return the deserialized sequenced event with source entity and sequence metadata
     * @throws DeserializationException if the record cannot be deserialized
     */
    SequencedEvent<T> deserialize(ConsumerRecord<String, byte[]> record) throws DeserializationException;
}
