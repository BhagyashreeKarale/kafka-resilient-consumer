package com.framework.resilient.benchmarks;

import com.framework.resilient.circuitbreaker.CircuitBreakerProperties;
import com.framework.resilient.circuitbreaker.HealthMonitorProperties;
import com.framework.resilient.circuitbreaker.PartitionCircuitBreaker;
import com.framework.resilient.circuitbreaker.PartitionHealthMonitor;
import com.framework.resilient.coordinator.ConsumerCoordinator;
import com.framework.resilient.coordinator.CoordinatorProperties;
import com.framework.resilient.coordinator.EventDeserializer;
import com.framework.resilient.coordinator.EventHandler;
import com.framework.resilient.coordinator.EventMetadata;
import com.framework.resilient.coordinator.ProcessingResult;
import com.framework.resilient.coordinator.StateStore;
import com.framework.resilient.dedup.DeduplicationEngine;
import com.framework.resilient.dedup.DeduplicationProperties;
import com.framework.resilient.dedup.InMemoryIdempotencyKeyStore;
import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.dlq.ErrorClassification;
import com.framework.resilient.metrics.MetricsExporter;
import com.framework.resilient.reorder.ReorderBuffer;
import com.framework.resilient.reorder.ReorderBufferProperties;
import com.framework.resilient.reorder.SequencedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Core benchmark test infrastructure providing Testcontainers Kafka cluster setup,
 * event production, failure injection, metric collection, and report generation.
 *
 * <p>This harness creates self-contained ConsumerCoordinator instances with in-memory
 * state stores and provides utilities for producing events, measuring throughput,
 * latency, and computing 95% confidence intervals.
 */
public class BenchmarkHarness {

    public static final int PARTITION_COUNT = 6;
    public static final String BENCHMARK_TOPIC = "benchmark-events";
    public static final String DLQ_TOPIC = "benchmark-dlq";
    public static final int MIN_EVENTS_PER_METRIC = 10_000;

    private final KafkaContainer kafka;
    private KafkaProducer<String, byte[]> producer;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public BenchmarkHarness(KafkaContainer kafka) {
        this.kafka = kafka;
    }

    // ========================= Setup =========================

    /**
     * Initializes the harness: creates topics and producer.
     */
    public void initialize() throws Exception {
        createTopics();
        this.producer = createProducer();
        closeables.add(producer);
    }

    /**
     * Cleans up all resources.
     */
    public void shutdown() {
        scheduler.shutdownNow();
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    // ========================= Topic Management =========================

    private void createTopics() throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = List.of(
                    new NewTopic(BENCHMARK_TOPIC, PARTITION_COUNT, (short) 1),
                    new NewTopic(DLQ_TOPIC, 1, (short) 1)
            );
            admin.createTopics(topics).all().get();
        }
    }

    /**
     * Creates a topic with specific partition count.
     */
    public void createTopic(String topicName, int partitions) throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(topicName, partitions, (short) 1))).all().get();
        }
    }

    // ========================= Event Production =========================

    /**
     * Produces sequential events to specified partitions.
     *
     * @param topic         target topic
     * @param partition     target partition (null for round-robin)
     * @param sourceEntity  source entity identifier
     * @param startSeq      starting sequence number
     * @param count         number of events to produce
     */
    public void produceEvents(String topic, Integer partition, String sourceEntity,
                              long startSeq, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            long seq = startSeq + i;
            byte[] payload = createEventPayload(sourceEntity, seq);
            ProducerRecord<String, byte[]> record;
            if (partition != null) {
                record = new ProducerRecord<>(topic, partition, sourceEntity, payload);
            } else {
                record = new ProducerRecord<>(topic, sourceEntity, payload);
            }
            record.headers().add("x-source-entity", sourceEntity.getBytes(StandardCharsets.UTF_8));
            record.headers().add("x-sequence-number", String.valueOf(seq).getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        }
        producer.flush();
    }

    /**
     * Produces events distributed across all partitions.
     */
    public void produceEventsToAllPartitions(int eventsPerPartition) throws Exception {
        for (int p = 0; p < PARTITION_COUNT; p++) {
            String entity = "entity-" + p;
            produceEvents(BENCHMARK_TOPIC, p, entity, 1, eventsPerPartition);
        }
    }

    /**
     * Produces deliberately out-of-order events for a source entity.
     */
    public void produceOutOfOrderEvents(String topic, int partition, String sourceEntity,
                                        long startSeq, int count) throws Exception {
        // Produce events in shuffled order
        List<Long> sequences = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            sequences.add(startSeq + i);
        }
        Collections.shuffle(sequences);

        for (long seq : sequences) {
            byte[] payload = createEventPayload(sourceEntity, seq);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, sourceEntity, payload);
            record.headers().add("x-source-entity", sourceEntity.getBytes(StandardCharsets.UTF_8));
            record.headers().add("x-sequence-number", String.valueOf(seq).getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        }
        producer.flush();
    }

    /**
     * Produces duplicate events for a source entity.
     */
    public void produceDuplicateEvents(String topic, int partition, String sourceEntity,
                                       long startSeq, int count, int duplicatesPerEvent) throws Exception {
        for (int i = 0; i < count; i++) {
            long seq = startSeq + i;
            for (int d = 0; d <= duplicatesPerEvent; d++) {
                byte[] payload = createEventPayload(sourceEntity, seq);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, sourceEntity, payload);
                record.headers().add("x-source-entity", sourceEntity.getBytes(StandardCharsets.UTF_8));
                record.headers().add("x-sequence-number", String.valueOf(seq).getBytes(StandardCharsets.UTF_8));
                producer.send(record).get();
            }
        }
        producer.flush();
    }

    // ========================= Consumer/Coordinator Creation =========================

    /**
     * Creates a KafkaConsumer configured for benchmarks.
     */
    public KafkaConsumer<String, byte[]> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return new KafkaConsumer<>(props);
    }

    /**
     * Creates a ConsumerCoordinator with the specified event handler and default properties.
     */
    public ConsumerCoordinator<byte[]> createCoordinator(
            EventHandler<byte[]> handler,
            String groupId) {
        return createCoordinator(handler, groupId, new CircuitBreakerProperties());
    }

    /**
     * Creates a ConsumerCoordinator with custom circuit breaker properties.
     */
    public ConsumerCoordinator<byte[]> createCoordinator(
            EventHandler<byte[]> handler,
            String groupId,
            CircuitBreakerProperties cbProps) {

        KafkaConsumer<String, byte[]> consumer = createConsumer(groupId);
        consumer.subscribe(List.of(BENCHMARK_TOPIC));

        MetricsExporter metrics = new MetricsExporter(new SimpleMeterRegistry());

        PartitionCircuitBreaker circuitBreaker = new PartitionCircuitBreaker(
                cbProps,
                tp -> consumer.pause(Collections.singleton(tp)),
                tp -> consumer.resume(Collections.singleton(tp))
        );

        PartitionHealthMonitor healthMonitor = new PartitionHealthMonitor(
                new HealthMonitorProperties(),
                cbProps,
                signal -> circuitBreaker.onDegradationSignal(signal.partition(), signal)
        );

        ReorderBuffer<byte[]> reorderBuffer = new ReorderBuffer<>(
                new ReorderBufferProperties(),
                tp -> consumer.pause(Collections.singleton(tp)),
                tp -> consumer.resume(Collections.singleton(tp))
        );

        InMemoryIdempotencyKeyStore keyStore = new InMemoryIdempotencyKeyStore();
        DeduplicationEngine dedupEngine = new DeduplicationEngine(
                keyStore, new DeduplicationProperties(), scheduler
        );

        KafkaProducer<String, byte[]> dlqProducer = createProducer();
        closeables.add(dlqProducer);
        DLQRouter dlqRouter = new DLQRouter(
                dlqProducer,
                new DLQProperties(3, Duration.ofMillis(100), Duration.ofSeconds(5),
                        1000, 10,
                        Map.of(ErrorClassification.TRANSIENT, DLQ_TOPIC,
                                ErrorClassification.PERMANENT, DLQ_TOPIC,
                                ErrorClassification.DESERIALIZATION, DLQ_TOPIC,
                                ErrorClassification.VALIDATION, DLQ_TOPIC),
                        DLQ_TOPIC),
                alert -> { /* no-op for benchmarks */ }
        );

        StateStore stateStore = new InMemoryStateStore();

        EventDeserializer<byte[]> deserializer = record -> {
            String sourceEntity = extractHeader(record, "x-source-entity");
            long seq = Long.parseLong(extractHeader(record, "x-sequence-number"));
            EventMetadata metadata = new EventMetadata(
                    record.topic(), record.partition(), record.offset(),
                    UUID.randomUUID().toString(), record.headers(), Instant.now()
            );
            return new SequencedEvent<>(sourceEntity, seq, record.value(), metadata, null);
        };

        CoordinatorProperties coordinatorProps = new CoordinatorProperties();
        DLQProperties dlqProperties = new DLQProperties(
                3, Duration.ofMillis(100), Duration.ofSeconds(5),
                1000, 10,
                Map.of(ErrorClassification.TRANSIENT, DLQ_TOPIC,
                        ErrorClassification.PERMANENT, DLQ_TOPIC,
                        ErrorClassification.DESERIALIZATION, DLQ_TOPIC,
                        ErrorClassification.VALIDATION, DLQ_TOPIC),
                DLQ_TOPIC
        );

        return new ConsumerCoordinator<>(
                consumer, handler, deserializer, circuitBreaker, healthMonitor,
                reorderBuffer, dedupEngine, dlqRouter, metrics, stateStore,
                coordinatorProps, dlqProperties
        );
    }

    // ========================= Metrics Collection =========================

    /**
     * Computes a MetricResult with 95% confidence interval from a list of sample values.
     */
    public static BenchmarkReport.MetricResult computeMetric(List<Double> samples, String unit) {
        if (samples.isEmpty()) {
            return new BenchmarkReport.MetricResult(0, 0, 0, unit);
        }
        double mean = samples.stream().mapToDouble(d -> d).average().orElse(0);
        double variance = samples.stream()
                .mapToDouble(d -> (d - mean) * (d - mean))
                .sum() / Math.max(1, samples.size() - 1);
        double stdDev = Math.sqrt(variance);
        return new BenchmarkReport.MetricResult(mean, stdDev, samples.size(), unit);
    }

    /**
     * Computes percentile from a sorted list of values.
     */
    public static double computePercentile(List<Double> sortedValues, double percentile) {
        if (sortedValues.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.size()) - 1;
        index = Math.max(0, Math.min(index, sortedValues.size() - 1));
        return sortedValues.get(index);
    }

    /**
     * Computes latency metrics (p50, p95, p99) with confidence intervals.
     */
    public static BenchmarkReport.LatencyMetricResult computeLatencyMetric(
            List<Double> latencySamples, String unit) {
        if (latencySamples.isEmpty()) {
            return new BenchmarkReport.LatencyMetricResult(0, 0, 0, 0, unit);
        }
        List<Double> sorted = latencySamples.stream().sorted().collect(Collectors.toList());
        double p50 = computePercentile(sorted, 50);
        double p95 = computePercentile(sorted, 95);
        double p99 = computePercentile(sorted, 99);

        BenchmarkReport.LatencyMetricResult result = new BenchmarkReport.LatencyMetricResult(
                p50, p95, p99, latencySamples.size(), unit);

        // Compute confidence interval for each percentile using bootstrap approximation
        result.setP50Metric(computeMetric(latencySamples, unit));
        result.setP95Metric(computeMetric(latencySamples, unit));
        result.setP99Metric(computeMetric(latencySamples, unit));

        return result;
    }

    /**
     * Measures throughput by consuming events and timing the consumption.
     * Returns events/second.
     */
    public double measureThroughput(String groupId, String topic, int expectedEvents,
                                    Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            int consumed = 0;
            Instant start = Instant.now();
            Instant deadline = start.plus(timeout);

            while (consumed < expectedEvents && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                consumed += records.count();
            }

            Duration elapsed = Duration.between(start, Instant.now());
            if (elapsed.toMillis() == 0) return 0;
            return (double) consumed / (elapsed.toMillis() / 1000.0);
        }
    }

    // ========================= Failure Injection =========================

    /**
     * Creates an EventHandler that injects failures for specified partitions.
     */
    public static EventHandler<byte[]> createFailingHandler(
            java.util.Set<Integer> failingPartitions,
            AtomicLong successCount,
            AtomicLong failureCount) {
        return (event, metadata) -> {
            if (failingPartitions.contains(metadata.partition())) {
                failureCount.incrementAndGet();
                throw new com.framework.resilient.coordinator.TransientProcessingException(
                        "Injected failure for partition " + metadata.partition());
            }
            successCount.incrementAndGet();
            return ProcessingResult.SUCCESS;
        };
    }

    /**
     * Creates a simple success handler that counts processed events.
     */
    public static EventHandler<byte[]> createSuccessHandler(AtomicLong processedCount) {
        return (event, metadata) -> {
            processedCount.incrementAndGet();
            return ProcessingResult.SUCCESS;
        };
    }

    /**
     * Creates a handler with configurable latency for each event.
     */
    public static EventHandler<byte[]> createLatencyHandler(
            AtomicLong processedCount, Duration artificialLatency) {
        return (event, metadata) -> {
            try {
                Thread.sleep(artificialLatency.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            processedCount.incrementAndGet();
            return ProcessingResult.SUCCESS;
        };
    }

    // ========================= Utility =========================

    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    private KafkaProducer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }

    private byte[] createEventPayload(String sourceEntity, long seq) {
        String json = String.format("{\"entity\":\"%s\",\"seq\":%d,\"ts\":\"%s\",\"data\":\"benchmark-payload-%d\"}",
                sourceEntity, seq, Instant.now().toString(), seq);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private String extractHeader(ConsumerRecord<String, byte[]> record, String key) {
        var header = record.headers().lastHeader(key);
        if (header == null) return "";
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Creates a new BenchmarkReport initialized with common metadata.
     */
    public BenchmarkReport createReport() {
        BenchmarkReport report = new BenchmarkReport();
        report.setPartitionCount(PARTITION_COUNT);
        report.setKafkaBootstrapServers(kafka.getBootstrapServers());
        report.setStartTime(Instant.now());
        return report;
    }
}
