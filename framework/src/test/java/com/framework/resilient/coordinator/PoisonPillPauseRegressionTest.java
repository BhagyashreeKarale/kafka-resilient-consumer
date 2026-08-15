package com.framework.resilient.coordinator;

import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: when a partition's circuit breaker is unhealthy the coordinator should pause that partition
 * and other partitions should continue processing.
 */
public class PoisonPillPauseRegressionTest {

    @Test
    public void unhealthyPartitionIsPaused_otherPartitionsProcessed() throws Exception {
        // Arrange
        org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> consumerMock = mock(org.apache.kafka.clients.consumer.KafkaConsumer.class);

        PartitionCircuitBreaker circuitBreaker = mock(PartitionCircuitBreaker.class);
        when(circuitBreaker.isPartitionHealthy(new TopicPartition("topic", 0))).thenReturn(false);
        when(circuitBreaker.isPartitionHealthy(new TopicPartition("topic", 1))).thenReturn(true);

        ReorderBuffer< String> reorderBuffer = mock(ReorderBuffer.class);
        ReorderResult<String> rr = mock(ReorderResult.class);
        when(rr.releasedEvents()).thenReturn(java.util.Collections.emptyList());
        when(rr.possibleDuplicates()).thenReturn(java.util.Collections.emptyList());
        when(reorderBuffer.submit(any(), any())).thenReturn(rr);
        when(reorderBuffer.getBufferDepth(any())).thenReturn(0);

        EventHandler<String> handler = mock(EventHandler.class);
        when(handler.handle(any(), any())).thenReturn(ProcessingResult.SUCCESS);

        ConsumerCoordinator<String> coordinator = new ConsumerCoordinator<>(
                consumerMock,
                handler,
                record -> new SequencedEvent<>("entity", 1L, "payload", new EventMetadata(record.topic(), record.partition(), record.offset(), "corr", new org.apache.kafka.common.header.internals.RecordHeaders(), Instant.now()), null),
                circuitBreaker,
                mock(PartitionHealthMonitor.class),
                reorderBuffer,
                mock(DeduplicationEngine.class),
                new DLQRouter(mock(org.apache.kafka.clients.producer.KafkaProducer.class), new DLQProperties(), s -> {}),
                mock(MetricsExporter.class),
                mock(StateStore.class),
                new CoordinatorProperties(),
                new DLQProperties(),
                Collections.emptyList()
        );

        // Act: process one record on unhealthy partition (0) and another on healthy partition (1)
        java.lang.reflect.Method m = ConsumerCoordinator.class.getDeclaredMethod("processRecord", ConsumerRecord.class);
        m.setAccessible(true);

        ConsumerRecord<String, byte[]> r0 = new ConsumerRecord<>("topic", 0, 10L, "k", "v".getBytes());
        ConsumerRecord<String, byte[]> r1 = new ConsumerRecord<>("topic", 1, 11L, "k", "v".getBytes());

        m.invoke(coordinator, r0);
        m.invoke(coordinator, r1);

        // Assert: partition 0 was paused, partition 1 was processed by handler
        verify(consumerMock, times(1)).pause(Collections.singleton(new TopicPartition("topic", 0)));
        verify(handler, times(1)).handle(any(), any());
    }
}
