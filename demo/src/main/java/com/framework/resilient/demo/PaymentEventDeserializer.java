package com.framework.resilient.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.framework.resilient.coordinator.EventDeserializer;
import com.framework.resilient.coordinator.EventMetadata;
import com.framework.resilient.dlq.DeserializationException;
import com.framework.resilient.reorder.SequencedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.UUID;

/**
 * Deserializes raw Kafka records into PaymentEvent instances.
 * Extracts sourceEntity (accountId) and sequenceNumber for ordered processing.
 */
public class PaymentEventDeserializer implements EventDeserializer<PaymentEvent> {

    private final ObjectMapper objectMapper;

    public PaymentEventDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public SequencedEvent<PaymentEvent> deserialize(ConsumerRecord<String, byte[]> record)
            throws DeserializationException {
        try {
            PaymentEvent event = objectMapper.readValue(record.value(), PaymentEvent.class);

            EventMetadata metadata = new EventMetadata(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    UUID.randomUUID().toString(),
                    record.headers(),
                    Instant.now()
            );

            return new SequencedEvent<>(
                    event.accountId(),
                    event.sequenceNumber(),
                    event,
                    metadata,
                    null
            );
        } catch (Exception e) {
            throw new DeserializationException(
                    "Failed to deserialize PaymentEvent from partition=" + record.partition()
                            + " offset=" + record.offset(), e);
        }
    }
}
