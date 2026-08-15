package com.framework.resilient.benchmarks;

import com.framework.resilient.circuitbreaker.CircuitBreakerProperties;
import com.framework.resilient.coordinator.ConsumerCoordinator;
import com.framework.resilient.coordinator.EventHandler;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Measures baseline throughput with all partitions healthy and throughput
 * with isolated partitions, reporting percentage degradation.
 *
 * Validates: Requirements 5.5, 5.12, 5.13
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ThroughputBenchmark {

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
    void measureBaselineThroughput() throws Exception {
        // Produce minimum 10,000 events across all 6 partitions
        int eventsPerPartition = BenchmarkHarness.MIN_EVENTS_PER_METRIC / BenchmarkHarness.PARTITION_COUNT + 1;
        harness.produceEventsToAllPartitions(eventsPerPartition);

        // Measure throughput with all partitions healthy
        AtomicLong processedCount = new AtomicLong(0);
        List<Double> throughputSamples = new ArrayList<>();

        // Run multiple measurement windows for statistical significance
        for (int trial = 0; trial < 5; trial++) {
            String groupId = "baseline-throughput-" + trial + "-" + System.currentTimeMillis();
            Instant start = Instant.now();
            double throughput = harness.measureThroughput(
                    groupId, BenchmarkHarness.BENCHMARK_TOPIC,
                    eventsPerPartition * BenchmarkHarness.PARTITION_COUNT,
                    Duration.ofSeconds(30)
            );
            throughputSamples.add(throughput);
            processedCount.addAndGet((long) throughput);
        }

        BenchmarkReport report = harness.createReport();
        report.setBaselineThroughput(BenchmarkHarness.computeMetric(throughputSamples, "events/sec"));
        report.setTotalEventsProcessed(processedCount.get());
        report.setEndTime(Instant.now());
        report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

        // Verify minimum event count
        assertThat(throughputSamples).isNotEmpty();
        assertThat(report.getBaselineThroughput().getMean()).isGreaterThan(0);

        report.writeToFile(Path.of("build", "benchmark-reports", "baseline-throughput.json"));
    }

    @Test
    void measureThroughputWithIsolation() throws Exception {
        String topic = "isolation-throughput-test";
        harness.createTopic(topic, BenchmarkHarness.PARTITION_COUNT);
        
        int eventsPerPartition = BenchmarkHarness.MIN_EVENTS_PER_METRIC / BenchmarkHarness.PARTITION_COUNT + 1;
        for (int p = 0; p < BenchmarkHarness.PARTITION_COUNT; p++) {
            harness.produceEvents(topic, p, "entity-" + p, 1, eventsPerPartition);
        }

        Set<Integer> failingPartitions = Set.of(0, 1);
        AtomicLong healthyProcessed = new AtomicLong(0);
        AtomicLong failedProcessed = new AtomicLong(0);

        // Measure with failing handler that triggers circuit breaker isolation
        List<Double> isolatedSamples = new ArrayList<>();
        for (int trial = 0; trial < 3; trial++) {
            healthyProcessed.set(0);
            failedProcessed.set(0);
            
            // Use the harness's failing handler factory which throws TransientProcessingException
            // for events on failingPartitions, triggering the circuit breaker
            EventHandler<byte[]> failingHandler = BenchmarkHarness.createFailingHandler(
                    failingPartitions, healthyProcessed, failedProcessed);
            
            String groupId = "isolation-" + trial + "-" + System.currentTimeMillis();
            
            // Create a coordinator with aggressive circuit breaker settings
            CircuitBreakerProperties cbProps = new CircuitBreakerProperties(
                    0.3, // low threshold to trip quickly
                    Duration.ofMillis(5000),
                    Duration.ofSeconds(60), // long cooldown — stays isolated for measurement
                    10, Duration.ofSeconds(30), Duration.ofSeconds(2), Duration.ofSeconds(1)
            );
            
            ConsumerCoordinator<byte[]> coordinator = harness.createCoordinator(failingHandler, groupId, cbProps);
            coordinator.subscribe(List.of(topic));
            coordinator.start();
            
            // Wait for circuit breaker to trip on failing partitions using Awaitility
            // (non-deterministic — don't use fixed Thread.sleep)
            await().atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> failedProcessed.get() >= cbProps.probeBatchSize());
            
            // Measure throughput over a 5-second window after isolation is established
            long startCount = healthyProcessed.get();
            Thread.sleep(5_000);
            long endCount = healthyProcessed.get();
            double throughput = (endCount - startCount) / 5.0; // events/sec
            isolatedSamples.add(throughput);
            
            coordinator.shutdown(Duration.ofSeconds(5));
        }

        BenchmarkReport report = harness.createReport();
        report.setIsolatedThroughput(BenchmarkHarness.computeMetric(isolatedSamples, "events/sec"));
        report.getErrorInjectionParams().put("failingPartitions", failingPartitions.toString());
        report.getErrorInjectionParams().put("failingPartitionCount", failingPartitions.size());
        report.setEndTime(Instant.now());
        report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

        assertThat(isolatedSamples).isNotEmpty();
        report.writeToFile(Path.of("build", "benchmark-reports", "isolation-throughput.json"));
    }
}
