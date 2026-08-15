package com.framework.resilient.dlq;

import com.framework.resilient.coordinator.PermanentProcessingException;
import com.framework.resilient.coordinator.TransientProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DLQRouter}.
 * Uses a Mockito-mocked KafkaProducer to control send behavior without a real broker.
 */
class DLQRouterTest {

    private KafkaProducer<String, byte[]> mockProducer;
    private DLQProperties properties;
    private List<String> alerts;
    private DLQRouter router;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockProducer = mock(KafkaProducer.class);
        alerts = new ArrayList<>();

        properties = new DLQProperties(
                3,
                Duration.ofSeconds(1),
                Duration.ofSeconds(60),
                1000,
                10,
                Map.of(
                        ErrorClassification.TRANSIENT, "dlq.transient",
                        ErrorClassification.PERMANENT, "dlq.permanent",
                        ErrorClassification.DESERIALIZATION, "dlq.deserialization",
                        ErrorClassification.VALIDATION, "dlq.validation"
                ),
                "dlq.default"
        );

        router = new DLQRouter(mockProducer, properties, alerts::add);
    }

    // -----------------------------------------------------------------------
    // classify() tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("classify() maps TransientProcessingException to TRANSIENT")
    void classifyTransientProcessingException() {
        ErrorClassification result = router.classify(new TransientProcessingException("timeout"));
        assertEquals(ErrorClassification.TRANSIENT, result);
    }

    @Test
    @DisplayName("classify() maps PermanentProcessingException to PERMANENT")
    void classifyPermanentProcessingException() {
        ErrorClassification result = router.classify(new PermanentProcessingException("invalid format"));
        assertEquals(ErrorClassification.PERMANENT, result);
    }

    @Test
    @DisplayName("classify() maps DeserializationException to DESERIALIZATION")
    void classifyDeserializationException() {
        ErrorClassification result = router.classify(new DeserializationException("bad json"));
        assertEquals(ErrorClassification.DESERIALIZATION, result);
    }

    @Test
    @DisplayName("classify() maps ValidationException to VALIDATION")
    void classifyValidationException() {
        ErrorClassification result = router.classify(new ValidationException("missing field"));
        assertEquals(ErrorClassification.VALIDATION, result);
    }

    @Test
    @DisplayName("classify() maps unknown exceptions to PERMANENT")
    void classifyUnknownException() {
        ErrorClassification result = router.classify(new IllegalStateException("something unexpected"));
        assertEquals(ErrorClassification.PERMANENT, result);
    }

    // -----------------------------------------------------------------------
    // selectTopic() tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("selectTopic() returns configured topic for known classification")
    void selectTopicReturnsConfiguredTopic() {
        assertEquals("dlq.transient", router.selectTopic(ErrorClassification.TRANSIENT));
        assertEquals("dlq.permanent", router.selectTopic(ErrorClassification.PERMANENT));
        assertEquals("dlq.deserialization", router.selectTopic(ErrorClassification.DESERIALIZATION));
        assertEquals("dlq.validation", router.selectTopic(ErrorClassification.VALIDATION));
    }

    @Test
    @DisplayName("selectTopic() returns default topic for unmapped classification and triggers alert")
    void selectTopicReturnsDefaultForUnmapped() {
        // Create a router with empty topic mappings so all classifications are unmapped
        DLQProperties emptyMappings = new DLQProperties(
                3, Duration.ofSeconds(1), Duration.ofSeconds(60),
                1000, 10, Map.of(), "dlq.default"
        );
        DLQRouter routerWithNoMappings = new DLQRouter(mockProducer, emptyMappings, alerts::add);

        String topic = routerWithNoMappings.selectTopic(ErrorClassification.TRANSIENT);

        assertEquals("dlq.default", topic);
        assertEquals(1, alerts.size());
        assertTrue(alerts.get(0).contains("Unmapped error classification"));
        assertTrue(alerts.get(0).contains("dlq.default"));
    }

    // -----------------------------------------------------------------------
    // enrichHeaders() tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("enrichHeaders() adds all required headers")
    void enrichHeadersAddsAllRequiredHeaders() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "orders.events", 2, 42L, "key-1", "payload".getBytes(StandardCharsets.UTF_8));

        Instant failureTime = Instant.parse("2024-01-15T10:30:00Z");

        Headers headers = router.enrichHeaders(
                record, ErrorClassification.TRANSIENT,
                3, failureTime, "PaymentService", "corr-abc-123");

        assertEquals("orders.events", headerValue(headers, DLQHeaders.ORIGINAL_TOPIC));
        assertEquals("2", headerValue(headers, DLQHeaders.ORIGINAL_PARTITION));
        assertEquals("42", headerValue(headers, DLQHeaders.ORIGINAL_OFFSET));
        assertEquals("TRANSIENT", headerValue(headers, DLQHeaders.ERROR_CLASSIFICATION));
        assertEquals("2024-01-15T10:30:00Z", headerValue(headers, DLQHeaders.FAILURE_TIMESTAMP));
        assertEquals("3", headerValue(headers, DLQHeaders.RETRY_COUNT));
        assertEquals("PaymentService", headerValue(headers, DLQHeaders.SOURCE_ENTITY));
        assertEquals("corr-abc-123", headerValue(headers, DLQHeaders.CORRELATION_ID));
    }

    // -----------------------------------------------------------------------
    // route() tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("route() successfully produces to DLQ and returns ROUTED")
    void routeSuccessReturnsRouted() {
        // Stub producer.send() to return a completed future
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("dlq.transient", 0), 0L, 0, 0L, 0, 0);
        when(mockProducer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "orders.events", 0, 10L, "key-1", "data".getBytes(StandardCharsets.UTF_8));

        DLQRoutingResult result = router.route(
                record, new TransientProcessingException("timeout"),
                ErrorClassification.TRANSIENT, 3, "OrderService", "corr-1");

        assertEquals(DLQRoutingResult.ROUTED, result);

        // Verify the producer was called with the correct topic
        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture());
        assertEquals("dlq.transient", captor.getValue().topic());
    }

    @Test
    @DisplayName("route() buffers locally when DLQ unavailable and returns BUFFERED")
    void routeBuffersWhenDlqUnavailable() {
        // Stub producer.send() to return a failed future
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(mockProducer.send(any())).thenReturn(failedFuture);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "orders.events", 0, 10L, "key-1", "data".getBytes(StandardCharsets.UTF_8));

        DLQRoutingResult result = router.route(
                record, new TransientProcessingException("timeout"),
                ErrorClassification.TRANSIENT, 3, "OrderService", "corr-1");

        assertEquals(DLQRoutingResult.BUFFERED, result);
        assertEquals(1, router.getBufferSize());
    }

    @Test
    @DisplayName("route() returns BUFFER_FULL when local buffer exceeds 1000 events")
    void routeReturnsBufferFullWhenCapacityExceeded() {
        // Stub producer to always fail
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(mockProducer.send(any())).thenReturn(failedFuture);

        // Fill the buffer to capacity
        for (int i = 0; i < 1000; i++) {
            ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                    "orders.events", 0, (long) i, "key-" + i, "data".getBytes(StandardCharsets.UTF_8));
            router.route(record, new TransientProcessingException("timeout"),
                    ErrorClassification.TRANSIENT, 1, "Svc", "corr-" + i);
        }

        assertEquals(1000, router.getBufferSize());
        assertTrue(router.isBufferFull());

        // Next event should be rejected
        ConsumerRecord<String, byte[]> overflow = new ConsumerRecord<>(
                "orders.events", 0, 1001L, "key-overflow", "data".getBytes(StandardCharsets.UTF_8));

        DLQRoutingResult result = router.route(
                overflow, new TransientProcessingException("timeout"),
                ErrorClassification.TRANSIENT, 1, "Svc", "corr-overflow");

        assertEquals(DLQRoutingResult.BUFFER_FULL, result);
        assertTrue(alerts.stream().anyMatch(a -> a.contains("buffer full")));
    }

    // -----------------------------------------------------------------------
    // retryBuffered() tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("retryBuffered() successfully sends buffered events and removes them")
    void retryBufferedSendsAndRemoves() {
        // Use zero-backoff properties so retries fire immediately
        DLQProperties zeroBackoffProps = new DLQProperties(
                3, Duration.ZERO, Duration.ZERO, 1000, 10,
                properties.topicMappings(), "dlq.default"
        );
        DLQRouter zeroBackoffRouter = new DLQRouter(mockProducer, zeroBackoffProps, alerts::add);

        // First, make send fail to buffer the event
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(mockProducer.send(any())).thenReturn(failedFuture);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "orders.events", 0, 10L, "key-1", "data".getBytes(StandardCharsets.UTF_8));
        zeroBackoffRouter.route(record, new TransientProcessingException("timeout"),
                ErrorClassification.TRANSIENT, 1, "Svc", "corr-1");
        assertEquals(1, zeroBackoffRouter.getBufferSize());

        // Now make send succeed and retry buffered
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("dlq.transient", 0), 0L, 0, 0L, 0, 0);
        when(mockProducer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));

        zeroBackoffRouter.retryBuffered();

        assertEquals(0, zeroBackoffRouter.getBufferSize());
    }

    // -----------------------------------------------------------------------
    // getBufferSize() and isBufferFull() tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getBufferSize() reflects current state")
    void getBufferSizeReflectsState() {
        assertEquals(0, router.getBufferSize());

        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(mockProducer.send(any())).thenReturn(failedFuture);

        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "orders.events", 0, 1L, "key", "data".getBytes(StandardCharsets.UTF_8));

        router.route(record, new TransientProcessingException("err"),
                ErrorClassification.TRANSIENT, 1, "Svc", "corr");
        assertEquals(1, router.getBufferSize());

        router.route(record, new TransientProcessingException("err"),
                ErrorClassification.TRANSIENT, 1, "Svc", "corr");
        assertEquals(2, router.getBufferSize());
    }

    @Test
    @DisplayName("isBufferFull() returns true at capacity")
    void isBufferFullReturnsTrueAtCapacity() {
        assertFalse(router.isBufferFull());

        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(mockProducer.send(any())).thenReturn(failedFuture);

        for (int i = 0; i < 1000; i++) {
            ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                    "orders.events", 0, (long) i, "key-" + i, "data".getBytes(StandardCharsets.UTF_8));
            router.route(record, new TransientProcessingException("err"),
                    ErrorClassification.TRANSIENT, 1, "Svc", "corr-" + i);
        }

        assertTrue(router.isBufferFull());
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    private String headerValue(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        assertNotNull(header, "Expected header '" + key + "' to be present");
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
