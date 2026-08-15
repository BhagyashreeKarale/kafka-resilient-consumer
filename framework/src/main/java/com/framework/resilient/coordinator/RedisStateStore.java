package com.framework.resilient.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot;
import com.framework.resilient.dedup.DeduplicationSnapshot;
import com.framework.resilient.reorder.BufferSnapshot;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Redis-backed implementation of {@link StateStore} for multi-instance deployments.
 *
 * <p>Persists circuit breaker, reorder buffer, and deduplication state to Redis,
 * enabling state sharing across consumer instances during partition reassignment.
 * When partition P moves from consumer A to consumer B, consumer B can restore
 * the circuit breaker state that consumer A persisted.
 *
 * <p>Key format: {@code state:{type}:{topic}:{partition}}
 * <br>Value: JSON-serialized state snapshot
 * <br>TTL: configurable (default 1 hour — state older than this is considered stale)
 *
 * <p><strong>Thread-safe:</strong> All operations delegate to {@link StringRedisTemplate}.
 *
 * <p><strong>Dependency:</strong> Requires {@code spring-boot-starter-data-redis} on the classpath.
 */
public class RedisStateStore implements StateStore {

    private static final Logger log = LoggerFactory.getLogger(RedisStateStore.class);

    private static final String CB_PREFIX = "state:cb:";
    private static final String RB_PREFIX = "state:rb:";
    private static final String DEDUP_PREFIX = "state:dedup:";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final Duration stateTtl;

    /**
     * Creates a Redis-backed state store.
     *
     * @param redisTemplate the Spring Redis template
     * @param objectMapper  Jackson mapper for JSON serialization
     * @param stateTtl      TTL for persisted state (state older than this is evicted)
     */
    public RedisStateStore(StringRedisTemplate redisTemplate, ObjectMapper objectMapper, Duration stateTtl) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.stateTtl = stateTtl;
    }

    @Override
    public void persistCircuitBreakerState(Map<TopicPartition, CircuitBreakerSnapshot> snapshots) {
        for (Map.Entry<TopicPartition, CircuitBreakerSnapshot> entry : snapshots.entrySet()) {
            String key = CB_PREFIX + partitionKey(entry.getKey());
            writeJson(key, entry.getValue());
        }
    }

    @Override
    public Optional<CircuitBreakerSnapshot> restoreCircuitBreakerState(TopicPartition partition) {
        String key = CB_PREFIX + partitionKey(partition);
        return readJson(key, CircuitBreakerSnapshot.class);
    }

    @Override
    public <T> void persistBufferState(Map<TopicPartition, BufferSnapshot<T>> snapshots) {
        for (Map.Entry<TopicPartition, BufferSnapshot<T>> entry : snapshots.entrySet()) {
            String key = RB_PREFIX + partitionKey(entry.getKey());
            writeJson(key, entry.getValue());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<BufferSnapshot<T>> restoreBufferState(TopicPartition partition) {
        String key = RB_PREFIX + partitionKey(partition);
        return readJson(key, BufferSnapshot.class).map(s -> (BufferSnapshot<T>) s);
    }

    @Override
    public void persistDeduplicationState(Map<TopicPartition, DeduplicationSnapshot> snapshots) {
        for (Map.Entry<TopicPartition, DeduplicationSnapshot> entry : snapshots.entrySet()) {
            String key = DEDUP_PREFIX + partitionKey(entry.getKey());
            writeJson(key, entry.getValue());
        }
    }

    @Override
    public Optional<DeduplicationSnapshot> restoreDeduplicationState(TopicPartition partition) {
        String key = DEDUP_PREFIX + partitionKey(partition);
        return readJson(key, DeduplicationSnapshot.class);
    }

    @Override
    public void removeState(TopicPartition partition) {
        String pk = partitionKey(partition);
        redisTemplate.delete(CB_PREFIX + pk);
        redisTemplate.delete(RB_PREFIX + pk);
        redisTemplate.delete(DEDUP_PREFIX + pk);
    }

    @Override
    public boolean isAvailable() {
        try (var conn = redisTemplate.getConnectionFactory().getConnection()) {
            String pong = conn.ping();
            return "PONG".equals(pong);
        } catch (Exception e) {
            log.warn("Redis state store health check failed: {}", e.getMessage());
            return false;
        }
    }

    private String partitionKey(TopicPartition partition) {
        return partition.topic() + ":" + partition.partition();
    }

    private void writeJson(String key, Object value) {
        try {
            String json = objectMapper.writeValueAsString(value);
            redisTemplate.opsForValue().set(key, json, stateTtl);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize state for key {}: {}", key, e.getMessage());
            throw new RuntimeException("Failed to serialize state", e);
        }
    }

    private <T> Optional<T> readJson(String key, Class<T> type) {
        String json = redisTemplate.opsForValue().get(key);
        if (json == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(json, type));
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize state for key {} (may be corrupted): {}", key, e.getMessage());
            return Optional.empty();
        }
    }
}
