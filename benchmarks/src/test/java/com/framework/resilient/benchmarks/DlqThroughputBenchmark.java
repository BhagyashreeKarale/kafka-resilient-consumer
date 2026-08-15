package com.framework.resilient.benchmarks;

import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.dlq.DLQRoutingResult;
import com.framework.resilient.dlq.ErrorClassification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures DLQ routing throughput: events/second routed to DLQ topics
 * while errors are continuously injected for a minimum of 60 seconds.
 *
 * Validates: Requirements 5.10, 5.12, 5.13
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DlqThroughputBenchmark {

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
    void measureDlqRoutingThroughput() throws Exception {
        String dlqTopic = "dlq-throughput-bench";
        harness.createTopic(dlqTopic, 1);

        // Create DLQ router pointed at the benchmark DLQ topic
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");

        KafkaProducer<String, byte[]> dlqProducer = new KafkaProducer<>(producerProps);
        AtomicLong alertCount = new AtomicLong(0);

        DLQProperties dlqProps = new DLQProperties(
                3, Duration.ofMillis(100), Duration.ofSeconds(5),
                1000, 10,
                Map.of(
                        ErrorClassification.TRANSIENT, dlqTopic,
                        ErrorClassification.PERMANENT, dlqTopic,
                        ErrorClassification.DESERIALIZATION, dlqTopic,
                        ErrorClassification.VALIDATION, dlqTopic
                ),
                dlqTopic
        );

        DLQRouter dlqRouter = new DLQRouter(dlqProducer, dlqProps, alert -> alertCount.incrementAndGet());

        // Continuously inject errors for 60+ seconds and measure DLQ routing throughput
        Duration minDuration = Duration.ofSeconds(60);
        Instant startTime = Instant.now();
        long routedCount = 0;
        List<Double> throughputSamples = new ArrayList<>();

        int windowEvents = 0;
        Instant windowStart = Instant.now();

        while (Duration.between(startTime, Instant.now()).compareTo(minDuration) < 0
                || routedCount < BenchmarkHarness.MIN_EVENTS_PER_METRIC) {

            // Create a fake consumer record representing a failed event
            byte[] payload = String.format("{\"error\":true,\"seq\":%d}", routedCount)
                    .getBytes(StandardCharsets.UTF_8);
            ConsumerRecord<String, byte[]> fakeRecord = new ConsumerRecord<>(
                    BenchmarkHarness.BENCHMARK_TOPIC, 0, routedCount,
                    "error-entity", payload
            );

            DLQRoutingResult result = dlqRouter.route(
                    fakeRecord,
                    new RuntimeException("Benchmark injected error #" + routedCount),
                    ErrorClassification.PERMANENT,
                    3,
                    "error-entity",
                    "correlation-" + routedCount
            );

            if (result == DLQRoutingResult.ROUTED) {
                routedCount++;
                windowEvents++;
            }

            // Compute throughput every 1000 events
            if (windowEvents >= 1000) {
                Duration windowDuration = Duration.between(windowStart, Instant.now());
                if (windowDuration.toMillis() > 0) {
                    double throughput = (double) windowEvents / (windowDuration.toMillis() / 1000.0);
                    throughputSamples.add(throughput);
                }
                windowEvents = 0;
                windowStart = Instant.now();
            }
        }

        // Final window
        if (windowEvents > 0) {
            Duration windowDuration = Duration.between(windowStart, Instant.now());
            if (windowDuration.toMillis() > 0) {
                double throughput = (double) windowEvents / (windowDuration.toMillis() / 1000.0);
                throughputSamples.add(throughput);
            }
        }

        Duration totalDuration = Duration.between(startTime, Instant.now());
        dlqProducer.close();

        BenchmarkReport report = harness.createReport();
        report.setDlqThroughput(BenchmarkHarness.computeMetric(throughputSamples, "events/sec"));
        report.setTotalEventsProcessed(routedCount);
        report.getErrorInjectionParams().put("minDurationSeconds", minDuration.toSeconds());
        report.getErrorInjectionParams().put("actualDurationMs", totalDuration.toMillis());
        report.getErrorInjectionParams().put("errorClassification", "PERMANENT");
        report.getErrorInjectionParams().put("totalRoutedEvents", routedCount);
        report.setEndTime(Instant.now());
        report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

        assertThat(routedCount).isGreaterThanOrEqualTo(BenchmarkHarness.MIN_EVENTS_PER_METRIC);
        assertThat(totalDuration).isGreaterThanOrEqualTo(minDuration);
        assertThat(report.getDlqThroughput().getMean()).isGreaterThan(0);

        report.writeToFile(Path.of("build", "benchmark-reports", "dlq-throughput.json"));
    }
}
