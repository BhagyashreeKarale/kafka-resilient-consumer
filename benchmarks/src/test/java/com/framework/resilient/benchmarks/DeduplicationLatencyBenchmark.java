package com.framework.resilient.benchmarks;

import com.framework.resilient.dedup.DeduplicationEngine;
import com.framework.resilient.dedup.DeduplicationProperties;
import com.framework.resilient.dedup.DeduplicationResult;
import com.framework.resilient.dedup.InMemoryIdempotencyKeyStore;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures deduplication latency: time from event receipt to duplicate discard decision.
 * Reports p50, p95, and p99 latencies.
 *
 * <p>This benchmark operates purely in-memory — no Kafka container needed.
 * It exercises the {@link DeduplicationEngine} directly.
 *
 * Validates: Requirements 5.6, 5.12, 5.13
 */
class DeduplicationLatencyBenchmark {

    @Test
    void measureDeduplicationLatency() throws Exception {
        List<Double> latencySamples = new ArrayList<>();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        try {
            InMemoryIdempotencyKeyStore keyStore = new InMemoryIdempotencyKeyStore();
            DeduplicationEngine dedupEngine = new DeduplicationEngine(
                    keyStore, new DeduplicationProperties(), scheduler
            );

            TopicPartition partition = new TopicPartition("benchmark-events", 0);
            int totalEvents = 10_000;
            String sourceEntity = "dedup-bench-entity";

            // Phase 1: Mark events as processed (populate the key store)
            for (int i = 0; i < totalEvents; i++) {
                dedupEngine.markProcessed(partition, sourceEntity, i);
            }

            // Phase 2: Check each event as a duplicate and measure latency
            for (int i = 0; i < totalEvents; i++) {
                Instant start = Instant.now();
                DeduplicationResult result = dedupEngine.check(partition, sourceEntity, i);
                Instant end = Instant.now();

                assertThat(result).isEqualTo(DeduplicationResult.DUPLICATE);

                double latencyNanos = Duration.between(start, end).toNanos();
                double latencyMs = latencyNanos / 1_000_000.0;
                latencySamples.add(latencyMs);
            }

            // Phase 3: Also measure latency for NEW_EVENT checks
            List<Double> newEventLatencies = new ArrayList<>();
            for (int i = totalEvents; i < totalEvents * 2; i++) {
                Instant start = Instant.now();
                DeduplicationResult result = dedupEngine.check(partition, sourceEntity, i);
                Instant end = Instant.now();

                assertThat(result).isEqualTo(DeduplicationResult.NEW_EVENT);

                double latencyNanos = Duration.between(start, end).toNanos();
                double latencyMs = latencyNanos / 1_000_000.0;
                newEventLatencies.add(latencyMs);
            }

            // Combine all dedup decision latencies
            List<Double> allLatencies = new ArrayList<>(latencySamples);
            allLatencies.addAll(newEventLatencies);

            BenchmarkReport report = new BenchmarkReport();
            report.setPartitionCount(6);
            report.setStartTime(Instant.now());
            report.setDeduplicationLatency(BenchmarkHarness.computeLatencyMetric(allLatencies, "ms"));
            report.setTotalEventsProcessed(allLatencies.size());
            report.getErrorInjectionParams().put("duplicateEvents", totalEvents);
            report.getErrorInjectionParams().put("newEvents", totalEvents);
            report.getErrorInjectionParams().put("keyStoreSize", keyStore.size());
            report.setEndTime(Instant.now());
            report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

            assertThat(allLatencies.size()).isGreaterThanOrEqualTo(10_000);
            assertThat(report.getDeduplicationLatency().getP50()).isGreaterThanOrEqualTo(0);
            assertThat(report.getDeduplicationLatency().getP95()).isGreaterThanOrEqualTo(report.getDeduplicationLatency().getP50());
            assertThat(report.getDeduplicationLatency().getP99()).isGreaterThanOrEqualTo(report.getDeduplicationLatency().getP95());

            report.writeToFile(Path.of("build", "benchmark-reports", "deduplication-latency.json"));

        } finally {
            scheduler.shutdownNow();
        }
    }
}
