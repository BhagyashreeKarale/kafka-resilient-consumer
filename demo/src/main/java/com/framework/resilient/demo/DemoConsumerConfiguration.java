package com.framework.resilient.demo;

import com.framework.resilient.circuitbreaker.CircuitBreakerProperties;
import com.framework.resilient.circuitbreaker.HealthMonitorProperties;
import com.framework.resilient.circuitbreaker.PartitionCircuitBreaker;
import com.framework.resilient.circuitbreaker.PartitionHealthMonitor;
import com.framework.resilient.coordinator.ConsumerCoordinator;
import com.framework.resilient.coordinator.CoordinatorProperties;
import com.framework.resilient.coordinator.FileBasedStateStore;
import com.framework.resilient.coordinator.StateStore;
import com.framework.resilient.dedup.DeduplicationEngine;
import com.framework.resilient.dedup.DeduplicationProperties;
import com.framework.resilient.dedup.InMemoryIdempotencyKeyStore;
import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.metrics.MetricsExporter;
import com.framework.resilient.reorder.ReorderBuffer;
import com.framework.resilient.reorder.ReorderBufferProperties;
import io.micrometer.core.instrument.MeterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PreDestroy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class DemoConsumerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(DemoConsumerConfiguration.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${demo.topic}")
    private String topic;

    private ConsumerCoordinator<PaymentEvent> coordinator;
    private ScheduledExecutorService dedupScheduler;

    @Bean
    public KafkaConsumer<String, byte[]> kafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        return new KafkaConsumer<>(props);
    }

    @Bean
    public KafkaProducer<String, byte[]> dlqProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    @Bean
    public ScheduledExecutorService deduplicationScheduler() {
        dedupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dedup-health-check");
            t.setDaemon(true);
            return t;
        });
        return dedupScheduler;
    }

    @Bean
    public PaymentEventDeserializer paymentEventDeserializer(ObjectMapper objectMapper) {
        return new PaymentEventDeserializer(objectMapper);
    }

    @Bean
    public ConsumerCoordinator<PaymentEvent> consumerCoordinator(
            KafkaConsumer<String, byte[]> kafkaConsumer,
            PaymentEventHandler paymentEventHandler,
            PaymentEventDeserializer paymentEventDeserializer,
            MeterRegistry meterRegistry,
            CoordinatorProperties coordinatorProperties,
            CircuitBreakerProperties circuitBreakerProperties,
            HealthMonitorProperties healthMonitorProperties,
            ReorderBufferProperties reorderBufferProperties,
            DeduplicationProperties deduplicationProperties,
            DLQProperties dlqProperties,
            KafkaProducer<String, byte[]> dlqProducer,
            ScheduledExecutorService deduplicationScheduler) {

        MetricsExporter metricsExporter = new MetricsExporter(meterRegistry);

        PartitionCircuitBreaker circuitBreaker = new PartitionCircuitBreaker(
                circuitBreakerProperties,
                tp -> kafkaConsumer.pause(Collections.singleton(tp)),
                tp -> kafkaConsumer.resume(Collections.singleton(tp))
        );

        PartitionHealthMonitor healthMonitor = new PartitionHealthMonitor(
                healthMonitorProperties,
                circuitBreakerProperties,
                signal -> circuitBreaker.onDegradationSignal(signal.partition(), signal)
        );

        ReorderBuffer<PaymentEvent> reorderBuffer = new ReorderBuffer<>(
                reorderBufferProperties,
                tp -> kafkaConsumer.pause(Collections.singleton(tp)),
                tp -> kafkaConsumer.resume(Collections.singleton(tp))
        );

        InMemoryIdempotencyKeyStore keyStore = new InMemoryIdempotencyKeyStore();
        DeduplicationEngine deduplicationEngine = new DeduplicationEngine(
                keyStore, deduplicationProperties, deduplicationScheduler);

        DLQRouter dlqRouter = new DLQRouter(dlqProducer, dlqProperties,
                alert -> log.warn("DLQ alert: {}", alert));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        // Demo uses file-based state in /tmp — acceptable for local development.
        // Production deployments should use a distributed StateStore implementation
        // (e.g., RedisStateStore, JdbcStateStore) for cross-instance state sharing.
        StateStore stateStore = new FileBasedStateStore(
                Path.of(System.getProperty("java.io.tmpdir"), "resilient-consumer-demo-state"),
                objectMapper);

        coordinator = new ConsumerCoordinator<>(
                kafkaConsumer, paymentEventHandler, paymentEventDeserializer,
                circuitBreaker, healthMonitor, reorderBuffer, deduplicationEngine,
                dlqRouter, metricsExporter, stateStore,
                coordinatorProperties, dlqProperties, List.of(topic)
        );
        coordinator.start();
        return coordinator;
    }

    @PreDestroy
    public void shutdown() {
        if (coordinator != null) {
            coordinator.shutdown(Duration.ofSeconds(10));
        }
        if (dedupScheduler != null) {
            dedupScheduler.shutdownNow();
        }
    }
}
