package com.framework.resilient.dedup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryIdempotencyKeyStoreTest {

    private InMemoryIdempotencyKeyStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryIdempotencyKeyStore();
    }

    @Test
    void containsReturnsFalseForUnknownKey() {
        assertThat(store.contains("unknown-key")).isFalse();
    }

    @Test
    void containsReturnsTrueAfterPut() {
        store.put("key-1", Instant.now());
        assertThat(store.contains("key-1")).isTrue();
    }

    @Test
    void putOverwritesExistingKey() {
        Instant first = Instant.parse("2024-01-01T00:00:00Z");
        Instant second = Instant.parse("2024-01-02T00:00:00Z");

        store.put("key-1", first);
        store.put("key-1", second);

        assertThat(store.size()).isEqualTo(1);
        assertThat(store.contains("key-1")).isTrue();
    }

    @Test
    void sizeReturnsZeroForEmptyStore() {
        assertThat(store.size()).isZero();
    }

    @Test
    void sizeReflectsNumberOfStoredKeys() {
        store.put("key-1", Instant.now());
        store.put("key-2", Instant.now());
        store.put("key-3", Instant.now());

        assertThat(store.size()).isEqualTo(3);
    }

    @Test
    void evictBeforeRemovesEntriesOlderThanCutoff() {
        Instant old = Instant.parse("2024-01-01T00:00:00Z");
        Instant recent = Instant.parse("2024-01-10T00:00:00Z");
        Instant cutoff = Instant.parse("2024-01-05T00:00:00Z");

        store.put("old-key", old);
        store.put("recent-key", recent);

        store.evictBefore(cutoff);

        assertThat(store.contains("old-key")).isFalse();
        assertThat(store.contains("recent-key")).isTrue();
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void evictBeforeDoesNotRemoveEntriesExactlyAtCutoff() {
        Instant exactCutoff = Instant.parse("2024-01-05T00:00:00Z");

        store.put("exact-key", exactCutoff);

        store.evictBefore(exactCutoff);

        assertThat(store.contains("exact-key")).isTrue();
    }

    @Test
    void evictBeforeOnEmptyStoreIsNoOp() {
        store.evictBefore(Instant.now());
        assertThat(store.size()).isZero();
    }

    @Test
    void healthCheckAlwaysReturnsTrue() {
        assertThat(store.healthCheck()).isTrue();
    }
}
