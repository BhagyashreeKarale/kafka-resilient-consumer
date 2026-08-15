package com.framework.resilient.dlq;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;

/**
 * An entry in the local DLQ buffer, representing an event that could not be
 * delivered to its target DLQ topic and is awaiting retry.
 *
 * @param producerRecord the enriched record to be produced to the DLQ topic
 * @param retryAttempts  number of DLQ production retry attempts made so far
 * @param firstAttempt   when the first DLQ production attempt was made
 * @param lastAttempt    when the most recent DLQ production attempt was made
 */
public record DLQEntry(
        ProducerRecord<String, byte[]> producerRecord,
        int retryAttempts,
        Instant firstAttempt,
        Instant lastAttempt
) {
}
