package com.framework.resilient.benchmarks;

import com.framework.resilient.coordinator.EventMetadata;
import com.framework.resilient.reorder.ReorderBuffer;
import com.framework.resilient.reorder.ReorderBufferProperties;
import com.framework.resilient.reorder.SequencedEvent;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures memory usage relative to reorder buffer size: bytes consumed per buffered event
 * at various buffer capacities.
 *
 * Validates: Requirements 5.11, 5.12, 5.13
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MemoryUsageBenchmark {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private BenchmarkHarness harness;

    @BeforeAll
    void setup() throws Exception {
        harness = new BenchmarkHarness(kafka);
        harness.initialize();
    }

    @AfterAll
    void teardown() {
        if (harness != null) {
            harness.shutdown();
        }
    }

    @Test
    void measureMemoryPerBufferedEvent() throws Exception {
        List<Double> bytesPerEventSamples = new ArrayList<>();
        TopicPartition partition = new TopicPartition(BenchmarkHarness.BENCHMARK_TOPIC, 0);

        // Test at various buffer capacities
        int[] bufferSizes = {1_000, 5_000, 10_000, 20_000, 50_000};

        for (int targetSize : bufferSizes) {
            // Force GC and measure baseline memory
            long memoryBefore = getUsedMemory();

            // Create a buffer and fill it to the target capacity
            // Use a high max to prevent backpressure from triggering
            ReorderBufferProperties props = new ReorderBufferProperties(
                    targetSize * 2, 0.8, Duration.ofSeconds(60)
            );
            ReorderBuffer<byte[]> buffer = new ReorderBuffer<>(
                    props,
                    tp -> {}, // no-op
                    tp -> {}  // no-op
            );
            buffer.initializePartition(partition);

            // Fill buffer with out-of-order events.
            // Strategy: for each source entity, submit seq=1 (accepted immediately to
            // initialize expected next=2), then submit seq=3,4,5... which get buffered
            // because seq=2 is missing.
            int entitiesCount = 100;
            int eventsPerEntity = targetSize / entitiesCount;

            for (int e = 0; e < entitiesCount; e++) {
                String sourceEntity = "memory-entity-" + e;
                // First event initializes the entity (seq=1 accepted, expectedNext=2)
                EventMetadata initMeta = new EventMetadata(
                        BenchmarkHarness.BENCHMARK_TOPIC, 0, 0,
                        UUID.randomUUID().toString(), null, Instant.now()
                );
                SequencedEvent<byte[]> initEvent = new SequencedEvent<>(
                        sourceEntity, 1, new byte[128], initMeta, Instant.now()
                );
                buffer.submit(partition, initEvent);

                // Now submit seq=3,4,5... which will all be buffered (gap at seq=2)
                for (int i = 0; i < eventsPerEntity; i++) {
                    long seq = i + 3; // Skip seq=2 to force buffering
                    byte[] payload = new byte[128]; // Fixed-size payload for consistent measurement
                    EventMetadata metadata = new EventMetadata(
                            BenchmarkHarness.BENCHMARK_TOPIC, 0, seq,
                            UUID.randomUUID().toString(), null, Instant.now()
                    );
                    SequencedEvent<byte[]> event = new SequencedEvent<>(
                            sourceEntity, seq, payload, metadata, Instant.now()
                    );
                    buffer.submit(partition, event);
                }
            }

            // Verify buffer is filled (approximately targetSize buffered events)
            assertThat(buffer.getBufferDepth(partition)).isGreaterThanOrEqualTo(targetSize - entitiesCount);

            // Measure memory after filling
            long memoryAfter = getUsedMemory();

            long memoryUsed = memoryAfter - memoryBefore;
            double bytesPerEvent = (double) memoryUsed / targetSize;

            // Only accept positive measurements (GC can cause anomalies)
            // Retry up to 3 times if measurement is negative
            int retries = 0;
            while (bytesPerEvent <= 0 && retries < 3) {
                memoryAfter = getUsedMemory();
                memoryUsed = memoryAfter - memoryBefore;
                bytesPerEvent = (double) memoryUsed / targetSize;
                retries++;
            }
            if (bytesPerEvent > 0) {
                bytesPerEventSamples.add(bytesPerEvent);
            }

            // Clean up buffer
            buffer.removePartition(partition);
        }

        BenchmarkReport report = harness.createReport();
        report.setMemoryBytesPerEvent(BenchmarkHarness.computeMetric(bytesPerEventSamples, "bytes/event"));
        report.setTotalEventsProcessed(
                java.util.Arrays.stream(bufferSizes).asLongStream().sum()
        );
        report.getErrorInjectionParams().put("bufferSizesTested", bufferSizes);
        report.getErrorInjectionParams().put("payloadSizeBytes", 128);
        report.getErrorInjectionParams().put("measurementMethod", "heap_delta");
        report.setEndTime(Instant.now());
        report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

        assertThat(bytesPerEventSamples).isNotEmpty();
        assertThat(report.getMemoryBytesPerEvent().getMean()).isGreaterThan(0);

        report.writeToFile(Path.of("build", "benchmark-reports", "memory-usage.json"));
    }

    private long getUsedMemory() {
        // Force garbage collection — calling multiple times increases probability of full collection
        // Note: System.gc() is a hint, not a guarantee. Multiple calls with pauses follow JMH best practice.
        for (int i = 0; i < 3; i++) {
            System.gc();
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}
