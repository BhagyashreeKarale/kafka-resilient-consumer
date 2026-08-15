package com.framework.resilient.dedup;

import java.time.Instant;

/**
 * Pluggable persistence interface for idempotency key tracking.
 * Implementations may be in-memory, backed by Redis, or any other store.
 */
public interface IdempotencyKeyStore {

    /**
     * Check if the given idempotency key has been previously recorded.
     *
     * @param key the idempotency key to check
     * @return true if the key exists in the store, false otherwise
     */
    boolean contains(String key);

    /**
     * Record an idempotency key with its processing timestamp.
     *
     * @param key         the idempotency key to store
     * @param processedAt when the event was processed
     */
    void put(String key, Instant processedAt);

    /**
     * Evict all keys that were processed before the given cutoff time.
     *
     * @param cutoff the cutoff instant; keys processed before this are removed
     */
    void evictBefore(Instant cutoff);

    /**
     * Perform a health check against the backing store.
     *
     * @return true if the store is available and healthy, false otherwise
     */
    boolean healthCheck();

    /**
     * Return the current number of keys stored.
     *
     * @return the number of idempotency keys in the store
     */
    long size();
}
