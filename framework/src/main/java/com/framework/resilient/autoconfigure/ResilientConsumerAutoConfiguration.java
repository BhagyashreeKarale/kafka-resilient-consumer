package com.framework.resilient.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.framework.resilient.circuitbreaker.CircuitBreakerProperties;
import com.framework.resilient.circuitbreaker.HealthMonitorProperties;
import com.framework.resilient.circuitbreaker.PartitionCircuitBreaker;
import com.framework.resilient.circuitbreaker.PartitionHealthMonitor;
import com.framework.resilient.coordinator.CoordinatorProperties;
import com.framework.resilient.coordinator.FileBasedStateStore;
import com.framework.resilient.coordinator.StateStore;
import com.framework.resilient.dedup.DeduplicationProperties;
import com.framework.resilient.dlq.DLQProperties;
import com.framework.resilient.dlq.DLQRouter;
import com.framework.resilient.metrics.MetricsExporter;
import com.framework.resilient.reorder.ReorderBufferProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Path;

/**
 * Spring Boot auto-configuration for the Resilient Consumer Framework.
 *
 * <p>Provides default beans for framework infrastructure components:
 * MetricsExporter, PartitionCircuitBreaker, PartitionHealthMonitor, DLQRouter, and FileBasedStateStore.
 *
 * <p><strong>Note:</strong> This auto-configuration does NOT create a {@code ConsumerCoordinator} bean.
 * The coordinator requires user-supplied {@code EventHandler<T>} and {@code EventDeserializer<T>}
 * implementations that are domain-specific. Users should build the ConsumerCoordinator themselves
 * using the auto-configured components provided here. Example:
 * <pre>{@code
 * @Bean
 * public ConsumerCoordinator<MyEvent> consumerCoordinator(
 *         PartitionCircuitBreaker circuitBreaker,
 *         PartitionHealthMonitor healthMonitor,
 *         DLQRouter dlqRouter,
 *         MetricsExporter metricsExporter,
 *         StateStore stateStore,
 *         CoordinatorProperties properties) {
 *     return new ConsumerCoordinator<>(
 *         kafkaConsumer, myEventHandler, myEventDeserializer,
 *         circuitBreaker, healthMonitor, reorderBuffer,
 *         deduplicationEngine, dlqRouter, metricsExporter,
 *         stateStore, properties);
 * }
 * }</pre>
 */
@AutoConfiguration
@EnableConfigurationProperties({
        CoordinatorProperties.class,
        CircuitBreakerProperties.class,
        HealthMonitorProperties.class,
        ReorderBufferProperties.class,
        DeduplicationProperties.class,
        DLQProperties.class
})
public class ResilientConsumerAutoConfiguration {

    /**
     * Provides a Jackson ObjectMapper configured with JavaTimeModule for Instant/Duration serialization.
     */
    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper resilientConsumerObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper;
    }

    /**
     * Provides the MetricsExporter for per-partition Micrometer metrics.
     */
    @Bean
    @ConditionalOnMissingBean
    public MetricsExporter metricsExporter(MeterRegistry meterRegistry) {
        return new MetricsExporter(meterRegistry);
    }

    /**
     * Provides the PartitionCircuitBreaker with no-op pause/resume callbacks.
     * The ConsumerCoordinator will wire the actual Kafka pause/resume callbacks at runtime.
     */
    @Bean
    @ConditionalOnMissingBean
    public PartitionCircuitBreaker partitionCircuitBreaker(CircuitBreakerProperties properties) {
        // Pause/resume callbacks are wired by the ConsumerCoordinator at runtime.
        // Providing no-op defaults here for standalone testing and health queries.
        return new PartitionCircuitBreaker(properties, partition -> {}, partition -> {});
    }

    /**
     * Provides the PartitionHealthMonitor wired to emit degradation signals to the circuit breaker.
     */
    @Bean
    @ConditionalOnMissingBean
    public PartitionHealthMonitor partitionHealthMonitor(
            HealthMonitorProperties healthProperties,
            CircuitBreakerProperties circuitBreakerProperties,
            PartitionCircuitBreaker circuitBreaker) {
        return new PartitionHealthMonitor(
                healthProperties,
                circuitBreakerProperties,
                signal -> circuitBreaker.onDegradationSignal(signal.partition(), signal));
    }

    /**
     * Provides the DLQRouter for routing failed events to classified DLQ topics.
     * Only created when a KafkaProducer bean is available in the context.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(KafkaProducer.class)
    public DLQRouter dlqRouter(KafkaProducer<String, byte[]> producer, DLQProperties properties) {
        return new DLQRouter(producer, properties, alert -> {});
    }

    /**
     * Provides the FileBasedStateStore for persisting circuit breaker, reorder buffer,
     * and deduplication state across rebalances.
     */
    @Bean
    @ConditionalOnMissingBean(StateStore.class)
    public FileBasedStateStore fileBasedStateStore(ObjectMapper objectMapper) {
        Path stateDir = Path.of(System.getProperty("java.io.tmpdir"), "resilient-consumer-state");
        return new FileBasedStateStore(stateDir, objectMapper);
    }
}
