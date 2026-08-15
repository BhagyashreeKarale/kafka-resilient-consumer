package com.framework.resilient.dlq;

import com.framework.resilient.coordinator.PermanentProcessingException;
import com.framework.resilient.coordinator.TransientProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Routes unprocessable events to classified Dead Letter Queue topics.
 * Handles error classification, topic selection, header enrichment,
 * and local buffering when DLQ topics are unavailable.
 */
public class DLQRouter {

    private static final int MAX_EXCEPTION_MESSAGE_LENGTH = 1000;

    private final KafkaProducer<String, byte[]> producer;
    private final DLQProperties properties;
    private final Consumer<String> alertCallback;
    private final ConcurrentLinkedQueue<DLQEntry> localBuffer;
    private final AtomicInteger bufferSize;

    /**
     * Constructs a DLQRouter with the given producer, properties, and alert callback.
     *
     * @param producer      Kafka producer for sending events to DLQ topics
     * @param properties    DLQ configuration properties
     * @param alertCallback callback invoked with alert messages for operational alerts
     */
    public DLQRouter(KafkaProducer<String, byte[]> producer, DLQProperties properties,
                     Consumer<String> alertCallback) {
        this.producer = producer;
        this.properties = properties;
        this.alertCallback = alertCallback;
        this.localBuffer = new ConcurrentLinkedQueue<>();
        this.bufferSize = new AtomicInteger(0);
    }

    /**
     * Classifies an error into one of the four DLQ error categories.
     *
     * @param error the throwable to classify
     * @return the error classification
     */
    public ErrorClassification classify(Throwable error) {
        if (error instanceof TransientProcessingException) {
            return ErrorClassification.TRANSIENT;
        } else if (error instanceof PermanentProcessingException) {
            return ErrorClassification.PERMANENT;
        } else if (error instanceof DeserializationException) {
            return ErrorClassification.DESERIALIZATION;
        } else if (error instanceof ValidationException) {
            return ErrorClassification.VALIDATION;
        } else {
            return ErrorClassification.PERMANENT;
        }
    }

    /**
     * Selects the target DLQ topic based on error classification.
     * Falls back to the default topic for unmapped classifications and emits an alert.
     *
     * @param classification the error classification
     * @return the target DLQ topic name
     */
    public String selectTopic(ErrorClassification classification) {
        String topic = properties.topicMappings().get(classification);
        if (topic == null) {
            alertCallback.accept("Unmapped error classification: " + classification
                    + ", routing to default topic: " + properties.defaultTopic());
            return properties.defaultTopic();
        }
        return topic;
    }

    /**
     * Enriches headers with DLQ metadata for traceability.
     *
     * @param record             the original consumer record
     * @param classification     the error classification
     * @param retryCount         number of processing retries attempted
     * @param failureTimestamp   when the failure occurred
     * @param sourceEntity       the source entity identifier
     * @param correlationId      correlation ID for tracing
     * @return enriched headers containing all DLQ metadata
     */
    public Headers enrichHeaders(ConsumerRecord<String, byte[]> record,
                                 ErrorClassification classification,
                                 int retryCount, Instant failureTimestamp,
                                 String sourceEntity, String correlationId) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(DLQHeaders.ORIGINAL_TOPIC,
                record.topic().getBytes(StandardCharsets.UTF_8));
        headers.add(DLQHeaders.ORIGINAL_PARTITION,
                String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8));
        headers.add(DLQHeaders.ORIGINAL_OFFSET,
                String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));
        headers.add(DLQHeaders.ERROR_CLASSIFICATION,
                classification.name().getBytes(StandardCharsets.UTF_8));
        headers.add(DLQHeaders.FAILURE_TIMESTAMP,
                failureTimestamp.toString().getBytes(StandardCharsets.UTF_8));
        headers.add(DLQHeaders.RETRY_COUNT,
                String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8));
        headers.add(DLQHeaders.SOURCE_ENTITY,
                (sourceEntity != null ? sourceEntity : "").getBytes(StandardCharsets.UTF_8));
        headers.add(DLQHeaders.CORRELATION_ID,
                (correlationId != null ? correlationId : "").getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    /**
     * Routes a failed event to the appropriate DLQ topic.
     * If the DLQ topic is unavailable, the event is buffered locally for retry.
     * If the buffer is full, returns BUFFER_FULL and emits an alert.
     *
     * @param originalRecord  the original consumer record that failed processing
     * @param error           the error that caused the failure
     * @param classification  the error classification
     * @param retryCount      number of processing retries attempted
     * @param sourceEntity    the source entity identifier
     * @param correlationId   correlation ID for tracing
     * @return the routing result indicating success, buffered, or buffer full
     */
    public DLQRoutingResult route(ConsumerRecord<String, byte[]> originalRecord,
                                  Throwable error, ErrorClassification classification,
                                  int retryCount, String sourceEntity,
                                  String correlationId) {
        Instant failureTimestamp = Instant.now();
        String topic = selectTopic(classification);
        Headers headers = enrichHeaders(originalRecord, classification, retryCount,
                failureTimestamp, sourceEntity, correlationId);

        // Add exception-specific headers
        RecordHeaders enrichedHeaders = new RecordHeaders(headers);
        enrichedHeaders.add(DLQHeaders.EXCEPTION_CLASS,
                error.getClass().getName().getBytes(StandardCharsets.UTF_8));
        String message = error.getMessage() != null ? error.getMessage() : "";
        if (message.length() > MAX_EXCEPTION_MESSAGE_LENGTH) {
            message = message.substring(0, MAX_EXCEPTION_MESSAGE_LENGTH);
        }
        enrichedHeaders.add(DLQHeaders.EXCEPTION_MESSAGE,
                message.getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(
                topic, null, originalRecord.key(), originalRecord.value(), enrichedHeaders);

        // Attempt to send to DLQ topic
        try {
            producer.send(producerRecord).get();
            return DLQRoutingResult.ROUTED;
        } catch (Exception e) {
            // DLQ unavailable — buffer locally
            return bufferEvent(producerRecord);
        }
    }

    /**
     * Retries sending buffered events with exponential backoff.
     * Removes successful entries from the buffer.
     * Emits a critical alert when an entry exceeds 10 retry attempts.
     */
    public void retryBuffered() {
        Iterator<DLQEntry> iterator = localBuffer.iterator();
        while (iterator.hasNext()) {
            DLQEntry entry = iterator.next();

            // Check exponential backoff: only retry if enough time has elapsed
            Duration backoff = computeBackoff(entry.retryAttempts());
            if (entry.lastAttempt().plus(backoff).isAfter(Instant.now())) {
                continue; // Not yet time to retry this entry
            }

            try {
                producer.send(entry.producerRecord()).get();
                // Success — remove from buffer
                iterator.remove();
                bufferSize.decrementAndGet();
            } catch (Exception e) {
                // Retry failed — update entry with incremented attempt count
                int newRetryCount = entry.retryAttempts() + 1;

                if (newRetryCount > properties.maxDlqRetryAttempts()) {
                    alertCallback.accept("CRITICAL: DLQ delivery permanently failed after "
                            + newRetryCount + " attempts for record to topic: "
                            + entry.producerRecord().topic());
                }

                // Replace entry with updated attempt count
                iterator.remove();
                DLQEntry updatedEntry = new DLQEntry(
                        entry.producerRecord(),
                        newRetryCount,
                        entry.firstAttempt(),
                        Instant.now()
                );
                localBuffer.add(updatedEntry);
                // bufferSize stays the same since we removed and re-added
            }
        }
    }

    /**
     * Returns the current number of events in the local buffer.
     *
     * @return buffer size
     */
    public int getBufferSize() {
        return bufferSize.get();
    }

    /**
     * Returns whether the local buffer is full (exceeds 1000 events).
     *
     * @return true if buffer is full
     */
    public boolean isBufferFull() {
        return bufferSize.get() >= properties.maxBufferSize();
    }

    private DLQRoutingResult bufferEvent(ProducerRecord<String, byte[]> producerRecord) {
        if (bufferSize.get() >= properties.maxBufferSize()) {
            alertCallback.accept("DLQ local buffer full (" + properties.maxBufferSize()
                    + " events). Rejecting new event for topic: " + producerRecord.topic());
            return DLQRoutingResult.BUFFER_FULL;
        }

        Instant now = Instant.now();
        DLQEntry entry = new DLQEntry(producerRecord, 1, now, now);
        localBuffer.add(entry);
        bufferSize.incrementAndGet();
        return DLQRoutingResult.BUFFERED;
    }

    private Duration computeBackoff(int retryAttempts) {
        // Exponential backoff: 1s base, doubles per attempt, capped at 60s
        long backoffMillis = properties.initialBackoff().toMillis()
                * (1L << Math.min(retryAttempts, 20)); // cap shift to prevent overflow
        long maxMillis = properties.maxBackoff().toMillis();
        return Duration.ofMillis(Math.min(backoffMillis, maxMillis));
    }
}
