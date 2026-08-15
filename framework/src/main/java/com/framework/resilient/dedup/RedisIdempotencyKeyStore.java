package com.framework.resilient.dedup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.Instant;

/**
 * Redis-backed implementation of {@link IdempotencyKeyStore} for multi-instance deployments.
 *
 * <p>Uses Redis SET operations with TTL-based expiry matching the configured retention period.
 * This enables cross-instance deduplication: when a partition moves from consumer A to consumer B
 * during rebalance, consumer B can detect duplicates that consumer A already processed.
 *
 * <p>Key format: {@code dedup:{sourceEntity}:{sequenceNumber}:{partition}}
 * <br>Value: ISO-8601 timestamp of when the event was processed
 * <br>TTL: matches the configured retention period (default 72h)
 *
 * <p><strong>Thread-safe:</strong> All operations delegate to {@link StringRedisTemplate}
 * which is thread-safe.
 *
 * <p><strong>Dependency:</strong> Requires {@code spring-boot-starter-data-redis} on the classpath.
 */
public class RedisIdempotencyKeyStore implements IdempotencyKeyStore {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyKeyStore.class);
    private static final String KEY_PREFIX = "dedup:";

    private final StringRedisTemplate redisTemplate;
    private final Duration ttl;

    /**
     * Creates a Redis-backed idempotency key store.
     *
     * @param redisTemplate the Spring Redis template for string operations
     * @param ttl           the time-to-live for keys (should match deduplication retention period)
     */
    public RedisIdempotencyKeyStore(StringRedisTemplate redisTemplate, Duration ttl) {
        this.redisTemplate = redisTemplate;
        this.ttl = ttl;
    }

    @Override
    public boolean contains(String key) {
        Boolean exists = redisTemplate.hasKey(KEY_PREFIX + key);
        return Boolean.TRUE.equals(exists);
    }

    @Override
    public void put(String key, Instant processedAt) {
        redisTemplate.opsForValue().set(
                KEY_PREFIX + key,
                processedAt.toString(),
                ttl
        );
    }

    @Override
    public void evictBefore(Instant cutoff) {
        // Redis handles expiry via TTL — no manual eviction needed.
        // Keys expire automatically when their TTL elapses.
        // This method is a no-op for Redis.
        log.debug("evictBefore called but Redis handles expiry via TTL — no-op");
    }

    @Override
    public boolean healthCheck() {
        try (var conn = redisTemplate.getConnectionFactory().getConnection()) {
            String pong = conn.ping();
            return "PONG".equals(pong);
        } catch (Exception e) {
            log.warn("Redis health check failed: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public long size() {
        // Note: Uses SCAN (cursor-based) instead of KEYS * to avoid blocking Redis.
        // KEYS * is O(N) and blocks the server — unacceptable in production.
        // This method is intended for metrics/monitoring, not hot-path use.
        long count = 0;
        try (var conn = redisTemplate.getConnectionFactory().getConnection()) {
            var options = org.springframework.data.redis.core.ScanOptions.scanOptions()
                    .match(KEY_PREFIX + "*")
                    .count(100)
                    .build();
            var cursor = conn.scan(options);
            while (cursor.hasNext()) {
                cursor.next();
                count++;
            }
        } catch (Exception e) {
            log.warn("Failed to scan Redis keys for size(): {}", e.getMessage());
        }
        return count;
    }
}
