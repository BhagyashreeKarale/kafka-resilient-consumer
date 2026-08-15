package com.framework.resilient.benchmarks;

import com.framework.resilient.coordinator.EventMetadata;
import com.framework.resilient.reorder.ReorderBuffer;
import com.framework.resilient.reorder.ReorderBufferProperties;
import com.framework.resilient.reorder.ReorderResult;
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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures reorder latency: time from event buffer entry to in-order release.
 * Reports p50, p95, and p99 latencies.
 *
 * Validates: Requirements 5.7, 5.12, 5.13
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReorderLatencyBenchmark {

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
    void measureReorderLatency() throws Exception {
        List<Double> latencySamples = new ArrayList<>();
        TopicPartition partition = new TopicPartition(BenchmarkHarness.BENCHMARK_TOPIC, 0);

        // We measure the time events spend buffered before being released in-order
        // Use a reorder buffer with reasonable timeout
        ReorderBufferProperties props = new ReorderBufferProperties(
                100_000, 0.8, Duration.ofSeconds(5)
        );
        ReorderBuffer<byte[]> buffer = new ReorderBuffer<>(
                props,
                tp -> {}, // no-op pause
                tp -> {}  // no-op resume
        );
        buffer.initializePartition(partition);

        int totalEvents = BenchmarkHarness.MIN_EVENTS_PER_METRIC;
        int batchSize = 100; // Produce in batches with gaps to trigger buffering
        int batches = totalEvents / batchSize;

        String sourceEntity = "reorder-bench-entity";

        for (int batch = 0; batch < batches; batch++) {
            long startSeq = (long) batch * batchSize + 1;

            // Create events in shuffled order to force buffering
            List<Long> sequences = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                sequences.add(startSeq + i);
            }
            Collections.shuffle(sequences);

            // Submit events and track buffer entry times
            for (long seq : sequences) {
                Instant bufferedAt = Instant.now();
                EventMetadata metadata = new EventMetadata(
                        BenchmarkHarness.BENCHMARK_TOPIC, 0, seq,
                        UUID.randomUUID().toString(), null, Instant.now()
                );
                SequencedEvent<byte[]> event = new SequencedEvent<>(
                        sourceEntity, seq, new byte[64], metadata, bufferedAt
                );

                ReorderResult<byte[]> result = buffer.submit(partition, event);

                // For released events, measure the latency from buffer entry to release
                for (SequencedEvent<byte[]> released : result.releasedEvents()) {
                    if (released.bufferedAt() != null) {
                        double latencyMs = Duration.between(released.bufferedAt(), Instant.now()).toNanos() / 1_000_000.0;
                        latencySamples.add(latencyMs);
                    }
                }
            }
        }

        // Also release timed-out events and measure their latency
        List<SequencedEvent<byte[]>> timedOut = buffer.releaseTimedOut(partition);
        for (SequencedEvent<byte[]> released : timedOut) {
            if (released.bufferedAt() != null) {
                double latencyMs = Duration.between(released.bufferedAt(), Instant.now()).toNanos() / 1_000_000.0;
                latencySamples.add(latencyMs);
            }
        }

        BenchmarkReport report = harness.createReport();
        report.setReorderLatency(BenchmarkHarness.computeLatencyMetric(latencySamples, "ms"));
        report.setTotalEventsProcessed(latencySamples.size());
        report.getErrorInjectionParams().put("batchSize", batchSize);
        report.getErrorInjectionParams().put("totalBatches", batches);
        report.getErrorInjectionParams().put("reorderTimeoutMs", props.reorderTimeout().toMillis());
        report.setEndTime(Instant.now());
        report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

        assertThat(latencySamples.size()).isGreaterThanOrEqualTo(BenchmarkHarness.MIN_EVENTS_PER_METRIC / 2);
        assertThat(report.getReorderLatency().getP50()).isGreaterThanOrEqualTo(0);
        assertThat(report.getReorderLatency().getP95()).isGreaterThanOrEqualTo(report.getReorderLatency().getP50());
        assertThat(report.getReorderLatency().getP99()).isGreaterThanOrEqualTo(report.getReorderLatency().getP95());

        report.writeToFile(Path.of("build", "benchmark-reports", "reorder-latency.json"));
    }
}
