package com.framework.resilient.benchmarks;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Measures rebalance duration: time from rebalance trigger (adding a second consumer)
 * to all partitions actively consuming across both consumers.
 *
 * Validates: Requirements 5.9, 5.12, 5.13
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RebalanceDurationBenchmark {

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
    void measureRebalanceDuration() throws Exception {
        String rebalanceTopic = "rebalance-bench-topic";
        harness.createTopic(rebalanceTopic, BenchmarkHarness.PARTITION_COUNT);

        // Pre-produce events so partitions have data to consume after rebalance
        int eventsPerPartition = BenchmarkHarness.MIN_EVENTS_PER_METRIC / BenchmarkHarness.PARTITION_COUNT + 1;
        for (int p = 0; p < BenchmarkHarness.PARTITION_COUNT; p++) {
            harness.produceEvents(rebalanceTopic, p, "rebalance-entity-" + p, 1, eventsPerPartition);
        }

        List<Double> rebalanceDurationSamples = new ArrayList<>();
        int trials = 5;

        for (int trial = 0; trial < trials; trial++) {
            String groupId = "rebalance-bench-" + trial + "-" + System.currentTimeMillis();
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                AtomicReference<Set<TopicPartition>> consumer1Partitions = new AtomicReference<>(Set.of());
                AtomicReference<Set<TopicPartition>> consumer2Partitions = new AtomicReference<>(Set.of());
                AtomicBoolean consumer1Running = new AtomicBoolean(true);
                AtomicBoolean consumer2Running = new AtomicBoolean(true);
                AtomicBoolean consumer1Signaled = new AtomicBoolean(false);
                CountDownLatch consumer1Ready = new CountDownLatch(1);

                // Start first consumer — it should get all 6 partitions
                executor.submit(() -> {
                    try (KafkaConsumer<String, byte[]> consumer = createBenchConsumer(groupId)) {
                        consumer.subscribe(List.of(rebalanceTopic));
                        while (consumer1Running.get()) {
                            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                            Set<TopicPartition> assignment = consumer.assignment();
                            consumer1Partitions.set(assignment);
                            if (assignment.size() > 0 && consumer1Signaled.compareAndSet(false, true)) {
                                consumer1Ready.countDown();
                            }
                        }
                    }
                });

                // Wait until consumer 1 has partitions assigned
                assertThat(consumer1Ready.await(30, TimeUnit.SECONDS)).isTrue();
                await().atMost(Duration.ofSeconds(15))
                        .until(() -> consumer1Partitions.get().size() == BenchmarkHarness.PARTITION_COUNT);

                // Trigger rebalance by adding second consumer
                Instant rebalanceTriggerTime = Instant.now();

                executor.submit(() -> {
                    try (KafkaConsumer<String, byte[]> consumer = createBenchConsumer(groupId)) {
                        consumer.subscribe(List.of(rebalanceTopic));
                        while (consumer2Running.get()) {
                            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                            consumer2Partitions.set(consumer.assignment());
                        }
                    }
                });

                // Wait until both consumers have partitions (all 6 covered)
                await().atMost(Duration.ofSeconds(60))
                        .pollInterval(Duration.ofMillis(200))
                        .until(() -> {
                            int total = consumer1Partitions.get().size() + consumer2Partitions.get().size();
                            return total == BenchmarkHarness.PARTITION_COUNT
                                    && consumer1Partitions.get().size() > 0
                                    && consumer2Partitions.get().size() > 0;
                        });

                Instant allPartitionsActive = Instant.now();
                double durationMs = Duration.between(rebalanceTriggerTime, allPartitionsActive).toMillis();
                rebalanceDurationSamples.add(durationMs);

                // Shutdown consumers
                consumer1Running.set(false);
                consumer2Running.set(false);

            } finally {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        BenchmarkReport report = harness.createReport();
        report.setRebalanceDurationMs(BenchmarkHarness.computeMetric(rebalanceDurationSamples, "ms"));
        report.setTotalEventsProcessed((long) eventsPerPartition * BenchmarkHarness.PARTITION_COUNT);
        report.getErrorInjectionParams().put("trials", trials);
        report.getErrorInjectionParams().put("partitionCount", BenchmarkHarness.PARTITION_COUNT);
        report.getErrorInjectionParams().put("rebalanceTrigger", "second consumer joining group");
        report.setEndTime(Instant.now());
        report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

        assertThat(rebalanceDurationSamples).hasSize(trials);
        assertThat(report.getRebalanceDurationMs().getMean()).isGreaterThan(0);

        report.writeToFile(Path.of("build", "benchmark-reports", "rebalance-duration.json"));
    }

    private KafkaConsumer<String, byte[]> createBenchConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return new KafkaConsumer<>(props);
    }
}
