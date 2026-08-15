package com.framework.resilient.coordinator;

import org.apache.kafka.common.header.Headers;

import java.time.Instant;

/**
 * Metadata associated with an event as it flows through the processing pipeline.
 *
 * @param topic           the Kafka topic the event was consumed from
 * @param partition       the partition number within the topic
 * @param offset          the offset of the record within the partition
 * @param correlationId   unique identifier for tracing the event across pipeline stages
 * @param originalHeaders the original Kafka record headers
 * @param ingestedAt      the timestamp when the event was ingested by the framework
 */
public record EventMetadata(
        String topic,
        int partition,
        long offset,
        String correlationId,
        Headers originalHeaders,
        Instant ingestedAt
) {
}
