package com.framework.resilient.coordinator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ThreadSafetyPauseTest {

    @Test
    public void pausePartition_enqueuesWhenCalledOffPollThread_and_runsOnDrain() throws Exception {
        // Arrange
        org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> consumerMock = mock(org.apache.kafka.clients.consumer.KafkaConsumer.class);
        ConsumerCoordinator<String> coordinator = new ConsumerCoordinator<>(
                consumerMock,
                (payload, meta) -> ProcessingResult.SUCCESS,
                record -> new SequencedEvent<>("entity", 1L, "p", new EventMetadata(record.topic(), record.partition(), record.offset(), "corr", new org.apache.kafka.common.header.internals.RecordHeaders(), Instant.now()), null),
                mock(PartitionCircuitBreaker.class),
                mock(PartitionHealthMonitor.class),
                mock(ReorderBuffer.class),
                mock(DeduplicationEngine.class),
                mock(DLQRouter.class),
                mock(MetricsExporter.class),
                mock(StateStore.class),
                new CoordinatorProperties(),
                new DLQProperties(),
                Collections.emptyList()
        );

        // Ensure pollThread is not current thread -> simulate off-poll call
        java.lang.reflect.Field pollThreadField = ConsumerCoordinator.class.getDeclaredField("pollThread");
        pollThreadField.setAccessible(true);
        pollThreadField.set(coordinator, new Thread());

        TopicPartition tp = new TopicPartition("topic", 0);

        // Act: call pausePartition from test thread (not poll thread)
        coordinator.pausePartition(tp);

        // Verify: consumer.pause was NOT called immediately
        verify(consumerMock, never()).pause(any());

        // Now drain the consumerActions queue by running the queued Runnable (simulate poll thread)
        java.lang.reflect.Field actionsField = ConsumerCoordinator.class.getDeclaredField("consumerActions");
        actionsField.setAccessible(true);
        java.util.concurrent.ConcurrentLinkedQueue<Runnable> q = (java.util.concurrent.ConcurrentLinkedQueue<Runnable>) actionsField.get(coordinator);
        Runnable r = q.poll();
        assertThat(r).isNotNull();
        r.run();

        // Now consumer.pause should have been invoked
        verify(consumerMock, times(1)).pause(Collections.singleton(tp));
    }
}
