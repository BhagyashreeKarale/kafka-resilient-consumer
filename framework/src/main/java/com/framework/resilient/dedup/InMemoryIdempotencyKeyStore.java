package com.framework.resilient.dedup;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link IdempotencyKeyStore} backed by a {@link ConcurrentHashMap}.
 * <p>
 * This is the default implementation suitable for single-instance deployments or testing.
 * Production deployments requiring shared state across instances should use a distributed
 * store implementation (e.g., Redis, RocksDB).
 * <p>
 * <strong>Important:</strong> This store is process-local. On rebalance, if a partition moves to
 * another consumer instance, that instance will not have this consumer's dedup state.
 * For multi-instance deployments, use a shared store implementation (e.g., Redis with TTL).
 * The {@code StateStore} persistence mechanism mitigates this for single-group rebalances
 * within the same instance, but cross-instance deduplication requires a distributed store.
 * <p>
 * Thread-safe for concurrent reads and writes.
 */
public class InMemoryIdempotencyKeyStore implements IdempotencyKeyStore {

    private final ConcurrentHashMap<String, Instant> store = new ConcurrentHashMap<>();

    @Override
    public boolean contains(String key) {
        return store.containsKey(key);
    }

    @Override
    public void put(String key, Instant processedAt) {
        store.put(key, processedAt);
    }

    @Override
    public void evictBefore(Instant cutoff) {
        store.entrySet().removeIf(entry -> entry.getValue().isBefore(cutoff));
    }

    @Override
    public boolean healthCheck() {
        return true;
    }

    @Override
    public long size() {
        return store.size();
    }

    /**
     * Returns all entries in the store. Used by DeduplicationEngine for partition-filtered snapshots.
     *
     * @return set of all key-timestamp entries
     */
    public Set<Map.Entry<String, Instant>> entries() {
        return store.entrySet();
    }
}
