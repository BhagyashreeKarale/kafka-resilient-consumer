package com.framework.resilient.dedup;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Idempotency key computation, duplicate detection, and backing store health management.
 * <p>
 * Computes composite idempotency keys from source entity, sequence number, and partition.
 * Detects duplicates by checking the backing {@link IdempotencyKeyStore}. When the store
 * becomes unavailable, signals the consumer coordinator to pause affected partitions and
 * periodically health-checks the store to resume upon recovery.
 * <p>
 * Thread-safe for concurrent access from the consumer poll loop.
 */
public class DeduplicationEngine {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationEngine.class);

    private static final int CONSECUTIVE_FAILURE_THRESHOLD = 3;

    private final IdempotencyKeyStore keyStore;
    private final DeduplicationProperties properties;
    private final ScheduledExecutorService healthCheckScheduler;
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private volatile boolean storeAvailable = true;

    private ScheduledFuture<?> healthCheckFuture;
    private Consumer<Collection<TopicPartition>> pauseCallback;
    private Supplier<Collection<TopicPartition>> partitionsSupplier;

    /**
     * Creates a new DeduplicationEngine.
     *
     * @param keyStore             the backing idempotency key store
     * @param properties           deduplication configuration properties
     * @param healthCheckScheduler executor for scheduling periodic health checks
     */
    public DeduplicationEngine(IdempotencyKeyStore keyStore,
                               DeduplicationProperties properties,
                               ScheduledExecutorService healthCheckScheduler) {
        this.keyStore = keyStore;
        this.properties = properties;
        this.healthCheckScheduler = healthCheckScheduler;
    }

    /**
     * Computes a composite idempotency key from the source entity, sequence number, and partition.
     *
     * @param sourceEntity   the source entity identifier
     * @param sequenceNumber the event sequence number
     * @param partition      the Kafka partition number
     * @return composite key in format "sourceEntity:sequenceNumber:partition"
     */
    public String computeKey(String sourceEntity, long sequenceNumber, int partition) {
        return sourceEntity + ":" + sequenceNumber + ":" + partition;
    }

    /**
     * Checks whether an event is a duplicate by looking up its idempotency key in the store.
     *
     * @param partition      the topic partition the event belongs to
     * @param sourceEntity   the source entity identifier
     * @param sequenceNumber the event sequence number
     * @return {@link DeduplicationResult#DUPLICATE} if the key exists, {@link DeduplicationResult#NEW_EVENT} otherwise
     */
    public DeduplicationResult check(TopicPartition partition, String sourceEntity, long sequenceNumber) {
        String key = computeKey(sourceEntity, sequenceNumber, partition.partition());
        if (keyStore.contains(key)) {
            log.debug("Duplicate detected for key '{}' on partition {}", key, partition);
            return DeduplicationResult.DUPLICATE;
        }
        return DeduplicationResult.NEW_EVENT;
    }

    /**
     * Marks an event as processed by storing its idempotency key.
     * Should be called after successful processing of a new event.
     *
     * @param partition      the topic partition the event belongs to
     * @param sourceEntity   the source entity identifier
     * @param sequenceNumber the event sequence number
     */
    public void markProcessed(TopicPartition partition, String sourceEntity, long sequenceNumber) {
        String key = computeKey(sourceEntity, sequenceNumber, partition.partition());
        keyStore.put(key, Instant.now());
        log.trace("Marked key '{}' as processed on partition {}", key, partition);
    }

    /**
     * Returns whether the backing idempotency key store is currently available.
     *
     * @return true if the store is available, false otherwise
     */
    public boolean isAvailable() {
        return storeAvailable;
    }

    /**
     * Evicts expired idempotency keys based on the configured retention period.
     * Keys older than the retention period are removed from the store.
     */
    public void evictExpired() {
        Instant cutoff = Instant.now().minus(properties.retentionPeriod());
        keyStore.evictBefore(cutoff);
        log.debug("Evicted expired keys with cutoff {}", cutoff);
    }

    /**
     * Starts periodic health checking of the backing store.
     * <p>
     * When the store becomes unavailable (3 consecutive failed health checks),
     * the pause callback is invoked with the current partition assignments.
     * When the store recovers, partitions are not automatically resumed — the
     * coordinator handles resumption based on store availability.
     *
     * @param pauseCallback      callback to pause partitions when store becomes unavailable
     * @param partitionsSupplier supplier of currently assigned partitions
     */
    public void startHealthCheck(Consumer<Collection<TopicPartition>> pauseCallback,
                                 Supplier<Collection<TopicPartition>> partitionsSupplier) {
        this.pauseCallback = pauseCallback;
        this.partitionsSupplier = partitionsSupplier;

        long intervalMillis = properties.healthCheckInterval().toMillis();
        this.healthCheckFuture = healthCheckScheduler.scheduleAtFixedRate(
                this::performHealthCheck,
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
        log.info("Started deduplication store health check with interval {}ms", intervalMillis);
    }

    /**
     * Stops the periodic health check scheduler.
     */
    public void stopHealthCheck() {
        if (healthCheckFuture != null) {
            healthCheckFuture.cancel(false);
            healthCheckFuture = null;
            log.info("Stopped deduplication store health check");
        }
    }

    /**
     * Creates a snapshot of the deduplication state for a given partition.
     * Filters the global key store for keys belonging to the specified partition
     * (key format: "sourceEntity:sequenceNumber:partition").
     *
     * @param partition the partition to snapshot
     * @return a snapshot containing all keys that belong to the partition
     */
    public DeduplicationSnapshot snapshot(TopicPartition partition) {
        String partitionSuffix = ":" + partition.partition();
        Map<String, Instant> partitionKeys = new HashMap<>();

        // The InMemoryIdempotencyKeyStore stores keys globally.
        // Filter by partition suffix to extract only this partition's keys.
        if (keyStore instanceof InMemoryIdempotencyKeyStore inMemoryStore) {
            for (Map.Entry<String, Instant> entry : inMemoryStore.entries()) {
                if (entry.getKey().endsWith(partitionSuffix)) {
                    partitionKeys.put(entry.getKey(), entry.getValue());
                }
            }
        }
        log.debug("Snapshot for partition {}: {} keys", partition, partitionKeys.size());
        return new DeduplicationSnapshot(partitionKeys);
    }

    /**
     * Restores deduplication state for a given partition from a previously persisted snapshot.
     *
     * @param partition the partition to restore state for
     * @param snapshot  the snapshot containing keys to restore
     */
    public void restore(TopicPartition partition, DeduplicationSnapshot snapshot) {
        if (snapshot == null || snapshot.keys() == null || snapshot.keys().isEmpty()) {
            log.debug("No deduplication state to restore for partition {}", partition);
            return;
        }
        for (Map.Entry<String, Instant> entry : snapshot.keys().entrySet()) {
            keyStore.put(entry.getKey(), entry.getValue());
        }
        log.info("Restored {} deduplication keys for partition {}", snapshot.keys().size(), partition);
    }

    /**
     * Returns the current number of keys in the backing store.
     *
     * @return the number of stored idempotency keys
     */
    public long getStoreSize() {
        return keyStore.size();
    }

    private void performHealthCheck() {
        try {
            boolean healthy = keyStore.healthCheck();
            if (healthy) {
                int previousFailures = consecutiveFailures.getAndSet(0);
                if (!storeAvailable) {
                    storeAvailable = true;
                    log.info("Deduplication store recovered after {} consecutive failures", previousFailures);
                }
            } else {
                int failures = consecutiveFailures.incrementAndGet();
                log.warn("Deduplication store health check failed (consecutive failures: {})", failures);
                if (failures >= CONSECUTIVE_FAILURE_THRESHOLD && storeAvailable) {
                    storeAvailable = false;
                    log.error("Deduplication store unavailable after {} consecutive health check failures, "
                            + "signaling partition pause", failures);
                    signalPause();
                }
            }
        } catch (Exception e) {
            int failures = consecutiveFailures.incrementAndGet();
            log.error("Deduplication store health check threw exception (consecutive failures: {})", failures, e);
            if (failures >= CONSECUTIVE_FAILURE_THRESHOLD && storeAvailable) {
                storeAvailable = false;
                log.error("Deduplication store unavailable after {} consecutive health check failures, "
                        + "signaling partition pause", failures);
                signalPause();
            }
        }
    }

    private void signalPause() {
        if (pauseCallback != null && partitionsSupplier != null) {
            Collection<TopicPartition> partitions = partitionsSupplier.get();
            if (partitions != null && !partitions.isEmpty()) {
                pauseCallback.accept(partitions);
                log.info("Signaled pause for {} partitions due to store unavailability", partitions.size());
            }
        }
    }
}
