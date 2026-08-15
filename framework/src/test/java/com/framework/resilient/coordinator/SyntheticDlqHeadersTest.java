package com.framework.resilient.coordinator;

import com.framework.resilient.dlq.ErrorClassification;
import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class SyntheticDlqHeadersTest {

    @Test
    public void syntheticDlqHasExpectedHeaders() {
        // Arrange
        DLQRouter mockRouter = mock(DLQRouter.class);
        DLQProperties props = new DLQProperties();
        // Create a coordinator but we will call routeToDlq via reflection or by making it package-private.
        KafkaConsumerStub consumerStub = new KafkaConsumerStub();
        ConsumerCoordinator<String> coordinator = new ConsumerCoordinator<>(
                consumerStub,
                (payload, meta) -> ProcessingResult.SUCCESS,
                (record) -> new SequencedEvent<>("entity-1", 1L, "payload", meta(record), null),
                mock(PartitionCircuitBreaker.class),
                mock(PartitionHealthMonitor.class),
                mock(ReorderBuffer.class),
                mock(DeduplicationEngine.class),
                new DLQRouterStub(),
                mock(MetricsExporter.class),
                mock(StateStore.class),
                new CoordinatorProperties(),
                new DLQProperties(),
                null
        );

        // Act
        // Call routeToDlq using a synthetic error path --- use reflection since routeToDlq is private
        Throwable error = new RuntimeException("boom");

        try {
            java.lang.reflect.Method m = ConsumerCoordinator.class.getDeclaredMethod("routeToDlq",
                    ConsumerRecord.class, Throwable.class, ErrorClassification.class, int.class, String.class, String.class);
            m.setAccessible(true);
            m.invoke(coordinator, null, error, ErrorClassification.TRANSIENT, 1, "entity-1", "corr-1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Assert: our DLQRouterStub captured the last routed record
        ConsumerRecord<String, byte[]> last = ((DLQRouterStub) coordinator.dlqRouter).lastRecord;
        assertThat(last).isNotNull();
        Headers headers = last.headers();
        assertThat(new String(headers.lastHeader("dlq.source.entity").value(), StandardCharsets.UTF_8)).isEqualTo("entity-1");
        assertThat(new String(headers.lastHeader("dlq.correlation.id").value(), StandardCharsets.UTF_8)).isEqualTo("corr-1");
        assertThat(new String(headers.lastHeader("dlq.error.classification").value(), StandardCharsets.UTF_8)).isEqualTo("TRANSIENT");
        assertThat(new String(headers.lastHeader("dlq.retry.count").value(), StandardCharsets.UTF_8)).isEqualTo("1");
        assertThat(new String(headers.lastHeader("dlq.origin").value(), StandardCharsets.UTF_8)).isEqualTo("reorder-buffer");
    }

    // Helper stubs and factories
    private static EventMetadata meta(ConsumerRecord<String, byte[]> r) {
        return new EventMetadata(r.topic(), r.partition(), r.offset(), "corr", new RecordHeaders(), java.time.Instant.now());
    }

    private static class DLQRouterStub extends DLQRouter {
        public ConsumerRecord<String, byte[]> lastRecord;

        public DLQRouterStub() {
            super(null, new DLQProperties(), s -> {});
        }

        @Override
        public com.framework.resilient.dlq.DLQRoutingResult route(ConsumerRecord<String, byte[]> originalRecord,
                                                                   Throwable error, com.framework.resilient.dlq.ErrorClassification classification,
                                                                   int retryCount, String sourceEntity, String correlationId) {
            this.lastRecord = originalRecord;
            return com.framework.resilient.dlq.DLQRoutingResult.ROUTED;
        }
    }

    // Minimal KafkaConsumer stub for unit tests
    private static class KafkaConsumerStub extends org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> {
        public KafkaConsumerStub() {
            super(java.util.Map.of(), null);
        }
    }
