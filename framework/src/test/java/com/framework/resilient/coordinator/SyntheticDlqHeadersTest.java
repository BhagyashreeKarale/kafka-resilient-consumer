package com.framework.resilient.coordinator;

import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.dlq.DLQRoutingResult;
import com.framework.resilient.dlq.ErrorClassification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Verifies that the synthetic DLQ path adds the expected headers.
 */
public class SyntheticDlqHeadersTest {

    @Test
    public void syntheticDlqHasExpectedHeaders() throws Exception {
        // Arrange
        org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> consumerMock = mock(org.apache.kafka.clients.consumer.KafkaConsumer.class);
        // Create a DLQRouter stub to capture the ProducerRecord passed to route()
        DLQRouterStub dlqStub = new DLQRouterStub();

        ConsumerCoordinator<String> coordinator = new ConsumerCoordinator<>(
                consumerMock,
                (payload, meta) -> ProcessingResult.SUCCESS,
                record -> new SequencedEvent<>("entity-1", 1L, "payload", new EventMetadata(record.topic(), record.partition(), record.offset(), "corr", new RecordHeaders(), Instant.now()), null),
                mock(PartitionCircuitBreaker.class),
                mock(PartitionHealthMonitor.class),
                mock(ReorderBuffer.class),
                mock(DeduplicationEngine.class),
                dlqStub,
                mock(MetricsExporter.class),
                mock(StateStore.class),
                new CoordinatorProperties(),
                new DLQProperties(),
                Collections.emptyList()
        );

        // Act: invoke private routeToDlq with null record to trigger synthetic path
        Throwable error = new RuntimeException("boom");
        java.lang.reflect.Method m = ConsumerCoordinator.class.getDeclaredMethod("routeToDlq",
                ConsumerRecord.class, Throwable.class, ErrorClassification.class, int.class, String.class, String.class);
        m.setAccessible(true);
        m.invoke(coordinator, null, error, ErrorClassification.TRANSIENT, 1, "entity-1", "corr-1");

        // Assert
        ConsumerRecord<String, byte[]> last = dlqStub.lastRecord;
        assertThat(last).isNotNull();
        Headers headers = last.headers();
        assertThat(new String(headers.lastHeader("dlq.source.entity").value(), StandardCharsets.UTF_8)).isEqualTo("entity-1");
        assertThat(new String(headers.lastHeader("dlq.correlation.id").value(), StandardCharsets.UTF_8)).isEqualTo("corr-1");
        assertThat(new String(headers.lastHeader("dlq.error.classification").value(), StandardCharsets.UTF_8)).isEqualTo("TRANSIENT");
        assertThat(new String(headers.lastHeader("dlq.retry.count").value(), StandardCharsets.UTF_8)).isEqualTo("1");
        assertThat(new String(headers.lastHeader("dlq.origin").value(), StandardCharsets.UTF_8)).isEqualTo("reorder-buffer");
    }

    // DLQRouter stub that captures the last routed record
    private static class DLQRouterStub extends DLQRouter {
        public ConsumerRecord<String, byte[]> lastRecord;

        public DLQRouterStub() {
            super(null, new DLQProperties(), s -> {});
        }

        @Override
        public DLQRoutingResult route(ConsumerRecord<String, byte[]> originalRecord,
                                      Throwable error, ErrorClassification classification,
                                      int retryCount, String sourceEntity, String correlationId) {
            this.lastRecord = originalRecord;
            return DLQRoutingResult.ROUTED;
        }
    }
}
