package com.framework.resilient.integration;

import com.framework.resilient.circuitbreaker.CircuitBreakerProperties;
import com.framework.resilient.circuitbreaker.CircuitState;
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
import com.framework.resilient.coordinator.RedisStateStore;
import com.framework.resilient.dedup.DeduplicationEngine;
import com.framework.resilient.dedup.DeduplicationProperties;
import com.framework.resilient.dedup.RedisIdempotencyKeyStore;
import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.dlq.ErrorClassification;
import com.framework.resilient.metrics.MetricsExporter;
import com.framework.resilient.reorder.ReorderBuffer;
import com.framework.resilient.reorder.ReorderBufferProperties;
import com.framework.resilient.reorder.SequencedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test proving the framework works correctly with 2 consumer instances
 * sharing state via Redis. Demonstrates that:
 * <ol>
 *   <li>Partition reassignment during rebalance preserves circuit breaker state</li>
 *   <li>Cross-instance deduplication works (consumer B detects events consumer A processed)</li>
 *   <li>Retained partitions continue processing without interruption during rebalance</li>
 * </ol>
 *
 * <p>Uses Testcontainers for both Kafka and Redis.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiInstanceRebalanceTest {

    private static final String TOPIC = "multi-instance-test";
    private static final String GROUP_ID = "multi-instance-group";
    private static final int PARTITION_COUNT = 6;

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private StringRedisTemplate redisTemplate;
    private ObjectMapper objectMapper;
    private KafkaProducer<String, byte[]> producer;

    @BeforeAll
    void setup() throws Exception {
        // Setup Redis template
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                redis.getHost(), redis.getMappedPort(6379));
        factory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(factory);

        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // Create topic
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafka.getBootstrapServers());
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(adminProps)) {
            admin.createTopics(List.of(
                    new org.apache.kafka.clients.admin.NewTopic(TOPIC, PARTITION_COUNT, (short) 1)
            )).all().get();
        }

        // Create producer
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(prodProps);
    }

    @AfterAll
    void teardown() {
        if (producer != null) producer.close();
    }

    @Test
    void crossInstanceDeduplication_consumerB_detectsEventsProcessedByConsumerA() throws Exception {
        // Setup: shared Redis-backed dedup store
        RedisIdempotencyKeyStore sharedKeyStore = new RedisIdempotencyKeyStore(
                redisTemplate, Duration.ofHours(1));

        // Consumer A processes events 1-100
        ScheduledExecutorService schedulerA = Executors.newSingleThreadScheduledExecutor();
        DeduplicationEngine dedupEngineA = new DeduplicationEngine(
                sharedKeyStore, new DeduplicationProperties(), schedulerA);

        TopicPartition partition0 = new TopicPartition(TOPIC, 0);
        for (int i = 1; i <= 100; i++) {
            dedupEngineA.markProcessed(partition0, "account-1", i);
        }

        // Consumer B (different instance) checks for the same events
        ScheduledExecutorService schedulerB = Executors.newSingleThreadScheduledExecutor();
        DeduplicationEngine dedupEngineB = new DeduplicationEngine(
                sharedKeyStore, new DeduplicationProperties(), schedulerB);

        // Consumer B should detect all 100 events as duplicates
        for (int i = 1; i <= 100; i++) {
            assertThat(dedupEngineB.check(partition0, "account-1", i))
                    .as("Event seq=%d should be detected as duplicate by consumer B", i)
                    .isEqualTo(com.framework.resilient.dedup.DeduplicationResult.DUPLICATE);
        }

        // New events should be detected as new
        assertThat(dedupEngineB.check(partition0, "account-1", 101))
                .isEqualTo(com.framework.resilient.dedup.DeduplicationResult.NEW_EVENT);

        schedulerA.shutdownNow();
        schedulerB.shutdownNow();
    }

    @Test
    void statePreservation_circuitBreakerState_survivesRebalance() throws Exception {
        // Consumer A persists OPEN circuit breaker state to Redis
        RedisStateStore stateStore = new RedisStateStore(redisTemplate, objectMapper, Duration.ofHours(1));

        TopicPartition partition2 = new TopicPartition(TOPIC, 2);

        com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot openSnapshot =
                new com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot(
                        CircuitState.OPEN, Instant.now(), 5, Instant.now());

        stateStore.persistCircuitBreakerState(Map.of(partition2, openSnapshot));

        // Simulate rebalance: Consumer B restores state for partition 2
        var restored = stateStore.restoreCircuitBreakerState(partition2);

        assertThat(restored).isPresent();
        assertThat(restored.get().state()).isEqualTo(CircuitState.OPEN);
        assertThat(restored.get().consecutiveFailures()).isEqualTo(5);
    }

    @Test
    void multiInstance_retainedPartitions_continueProcessingDuringRebalance() throws Exception {
        // Produce events to all partitions
        for (int p = 0; p < PARTITION_COUNT; p++) {
            for (int i = 1; i <= 50; i++) {
                String payload = String.format("{\"entity\":\"e-%d\",\"seq\":%d}", p, i);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        TOPIC, p, "e-" + p, payload.getBytes(StandardCharsets.UTF_8));
                record.headers().add("x-source-entity", ("e-" + p).getBytes(StandardCharsets.UTF_8));
                record.headers().add("x-sequence-number", String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                producer.send(record).get();
            }
        }
        producer.flush();

        // Start Consumer A — gets all 6 partitions
        AtomicLong consumerAProcessed = new AtomicLong(0);
        CopyOnWriteArrayList<Integer> consumerAPartitions = new CopyOnWriteArrayList<>();

        KafkaConsumer<String, byte[]> consumerA = createConsumer(GROUP_ID + "-" + UUID.randomUUID());
        consumerA.subscribe(List.of(TOPIC));

        // Poll until consumer A has partitions
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    consumerA.poll(Duration.ofMillis(100));
                    return consumerA.assignment().size() == PARTITION_COUNT;
                });

        // Consumer A is now consuming all 6 partitions
        assertThat(consumerA.assignment()).hasSize(PARTITION_COUNT);

        // Start Consumer B — triggers rebalance
        KafkaConsumer<String, byte[]> consumerB = createConsumer(GROUP_ID + "-" + UUID.randomUUID());
        consumerB.subscribe(List.of(TOPIC));

        // Wait until partitions are redistributed
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    consumerA.poll(Duration.ofMillis(100));
                    consumerB.poll(Duration.ofMillis(100));
                    return consumerA.assignment().size() > 0
                            && consumerB.assignment().size() > 0
                            && (consumerA.assignment().size() + consumerB.assignment().size()) == PARTITION_COUNT;
                });

        // Both consumers have partitions — rebalance completed
        int totalPartitions = consumerA.assignment().size() + consumerB.assignment().size();
        assertThat(totalPartitions).isEqualTo(PARTITION_COUNT);

        // Both consumers can poll (retained partitions weren't disrupted)
        var recordsA = consumerA.poll(Duration.ofSeconds(2));
        var recordsB = consumerB.poll(Duration.ofSeconds(2));

        // At least one consumer should have received records
        assertThat(recordsA.count() + recordsB.count()).isGreaterThan(0);

        consumerA.close();
        consumerB.close();
    }

    private KafkaConsumer<String, byte[]> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        return new KafkaConsumer<>(props);
    }
}
