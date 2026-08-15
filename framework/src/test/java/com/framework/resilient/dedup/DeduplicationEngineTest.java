package com.framework.resilient.dedup;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DeduplicationEngineTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);

    private InMemoryIdempotencyKeyStore keyStore;
    private ScheduledExecutorService scheduler;
    private DeduplicationEngine engine;
    private DeduplicationProperties properties;

    @BeforeEach
    void setUp() {
        keyStore = new InMemoryIdempotencyKeyStore();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        properties = new DeduplicationProperties(
                Duration.ofMillis(200),   // short retention for testing
                Duration.ofMillis(100),   // eviction interval
                Duration.ofMillis(50)     // health check interval
        );
        engine = new DeduplicationEngine(keyStore, properties, scheduler);
    }

    @AfterEach
    void tearDown() {
        engine.stopHealthCheck();
        scheduler.shutdownNow();
    }

    // --- Scenario 1: computeKey() produces correct composite key format ---

    @Test
    @DisplayName("computeKey produces composite key in format entity:seq:partition")
    void computeKey_producesCorrectFormat() {
        String key = engine.computeKey("order-123", 42L, 7);

        assertThat(key).isEqualTo("order-123:42:7");
    }

    @Test
    @DisplayName("computeKey handles edge cases like empty entity and zero values")
    void computeKey_handlesEdgeCases() {
        assertThat(engine.computeKey("", 0L, 0)).isEqualTo(":0:0");
        assertThat(engine.computeKey("a:b", 1L, 2)).isEqualTo("a:b:1:2");
    }

    // --- Scenario 2: check() returns NEW_EVENT for unseen keys ---

    @Test
    @DisplayName("check returns NEW_EVENT for a key not yet in the store")
    void check_returnsNewEvent_forUnseenKey() {
        DeduplicationResult result = engine.check(PARTITION_0, "payment-456", 1L);

        assertThat(result).isEqualTo(DeduplicationResult.NEW_EVENT);
    }

    // --- Scenario 3: check() returns DUPLICATE after markProcessed() ---

    @Test
    @DisplayName("check returns DUPLICATE after markProcessed for the same key")
    void check_returnsDuplicate_afterMarkProcessed() {
        engine.markProcessed(PARTITION_0, "payment-456", 1L);

        DeduplicationResult result = engine.check(PARTITION_0, "payment-456", 1L);

        assertThat(result).isEqualTo(DeduplicationResult.DUPLICATE);
    }

    @Test
    @DisplayName("check returns NEW_EVENT for different sequence on same entity")
    void check_returnsNewEvent_forDifferentSequence() {
        engine.markProcessed(PARTITION_0, "payment-456", 1L);

        DeduplicationResult result = engine.check(PARTITION_0, "payment-456", 2L);

        assertThat(result).isEqualTo(DeduplicationResult.NEW_EVENT);
    }

    @Test
    @DisplayName("check returns NEW_EVENT for same entity and seq but different partition")
    void check_returnsNewEvent_forDifferentPartition() {
        engine.markProcessed(PARTITION_0, "payment-456", 1L);

        DeduplicationResult result = engine.check(PARTITION_1, "payment-456", 1L);

        assertThat(result).isEqualTo(DeduplicationResult.NEW_EVENT);
    }

    // --- Scenario 4: markProcessed() stores the key in the backing store ---

    @Test
    @DisplayName("markProcessed stores key in backing store")
    void markProcessed_storesKeyInStore() {
        assertThat(keyStore.size()).isZero();

        engine.markProcessed(PARTITION_0, "order-789", 5L);

        assertThat(keyStore.size()).isEqualTo(1);
        assertThat(keyStore.contains("order-789:5:0")).isTrue();
    }

    // --- Scenario 5: evictExpired() removes keys older than retention period ---

    @Test
    @DisplayName("evictExpired removes keys older than retention period")
    void evictExpired_removesExpiredKeys() {
        // Put a key with a timestamp well in the past (older than 200ms retention)
        keyStore.put("old-key:1:0", Instant.now().minus(Duration.ofMillis(500)));
        assertThat(keyStore.size()).isEqualTo(1);

        engine.evictExpired();

        assertThat(keyStore.size()).isZero();
        assertThat(keyStore.contains("old-key:1:0")).isFalse();
    }

    // --- Scenario 6: evictExpired() does not remove keys within retention period ---

    @Test
    @DisplayName("evictExpired does not remove keys within retention period")
    void evictExpired_keepsRecentKeys() {
        // Put a key with a recent timestamp (within 200ms retention)
        keyStore.put("recent-key:1:0", Instant.now());
        assertThat(keyStore.size()).isEqualTo(1);

        engine.evictExpired();

        assertThat(keyStore.size()).isEqualTo(1);
        assertThat(keyStore.contains("recent-key:1:0")).isTrue();
    }

    @Test
    @DisplayName("evictExpired removes only expired keys, keeping recent ones")
    void evictExpired_selectivelyRemovesKeys() {
        keyStore.put("old-key:1:0", Instant.now().minus(Duration.ofMillis(500)));
        keyStore.put("recent-key:2:0", Instant.now());

        engine.evictExpired();

        assertThat(keyStore.size()).isEqualTo(1);
        assertThat(keyStore.contains("old-key:1:0")).isFalse();
        assertThat(keyStore.contains("recent-key:2:0")).isTrue();
    }

    // --- Scenario 7: isAvailable() returns true when store is healthy ---

    @Test
    @DisplayName("isAvailable returns true initially when store is healthy")
    void isAvailable_returnsTrueInitially() {
        assertThat(engine.isAvailable()).isTrue();
    }

    // --- Scenario 8: After 3 consecutive health check failures, isAvailable() returns false and pause callback is invoked ---

    @Test
    @DisplayName("After 3 consecutive health check failures, isAvailable returns false and pause callback is invoked")
    void isAvailable_returnsFalse_afterConsecutiveFailures_andPauseCallbackInvoked() {
        // Use a controllable store that can simulate failures
        AtomicBoolean healthy = new AtomicBoolean(false);
        IdempotencyKeyStore controllableStore = new DelegatingKeyStore(keyStore, healthy);

        DeduplicationEngine engineWithControllable = new DeduplicationEngine(
                controllableStore, properties, scheduler);

        List<Collection<TopicPartition>> pausedPartitions = new ArrayList<>();
        List<TopicPartition> assignedPartitions = List.of(PARTITION_0, PARTITION_1);

        engineWithControllable.startHealthCheck(
                pausedPartitions::add,
                () -> assignedPartitions
        );

        // Wait for at least 3 health check intervals to pass and the engine to detect failure
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    assertThat(engineWithControllable.isAvailable()).isFalse();
                    assertThat(pausedPartitions).isNotEmpty();
                });

        // Verify pause callback was invoked with the assigned partitions
        assertThat(pausedPartitions.get(0)).containsExactlyInAnyOrderElementsOf(assignedPartitions);

        engineWithControllable.stopHealthCheck();
    }

    // --- Scenario 9: After store recovers, isAvailable() returns true ---

    @Test
    @DisplayName("After store recovers from failures, isAvailable returns true")
    void isAvailable_returnsTrue_afterStoreRecovers() {
        AtomicBoolean healthy = new AtomicBoolean(false);
        IdempotencyKeyStore controllableStore = new DelegatingKeyStore(keyStore, healthy);

        DeduplicationEngine engineWithControllable = new DeduplicationEngine(
                controllableStore, properties, scheduler);

        List<Collection<TopicPartition>> pausedPartitions = new ArrayList<>();
        List<TopicPartition> assignedPartitions = List.of(PARTITION_0);

        engineWithControllable.startHealthCheck(
                pausedPartitions::add,
                () -> assignedPartitions
        );

        // Wait until store becomes unavailable
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(engineWithControllable.isAvailable()).isFalse());

        // Simulate recovery
        healthy.set(true);

        // Wait for health check to detect recovery
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(engineWithControllable.isAvailable()).isTrue());

        engineWithControllable.stopHealthCheck();
    }

    // --- Scenario 10: snapshot() and restore() round-trip works for a partition ---

    @Test
    @DisplayName("snapshot and restore round-trip preserves keys for a partition")
    void snapshotAndRestore_roundTrip() {
        // Create a snapshot with known keys
        Map<String, Instant> keys = Map.of(
                "entity-A:1:0", Instant.now().minus(Duration.ofMillis(50)),
                "entity-B:2:0", Instant.now().minus(Duration.ofMillis(30))
        );
        DeduplicationSnapshot snapshot = new DeduplicationSnapshot(keys);

        // Restore the snapshot
        engine.restore(PARTITION_0, snapshot);

        // Verify all keys from the snapshot are now in the store
        assertThat(keyStore.contains("entity-A:1:0")).isTrue();
        assertThat(keyStore.contains("entity-B:2:0")).isTrue();
        assertThat(keyStore.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("restore with null snapshot does not throw")
    void restore_withNullSnapshot_doesNotThrow() {
        engine.restore(PARTITION_0, null);

        assertThat(keyStore.size()).isZero();
    }

    @Test
    @DisplayName("restore with empty snapshot does not add keys")
    void restore_withEmptySnapshot_doesNotAddKeys() {
        engine.restore(PARTITION_0, new DeduplicationSnapshot(Map.of()));

        assertThat(keyStore.size()).isZero();
    }

    // --- Helper: Delegating store that controls healthCheck result ---

    /**
     * A test-only IdempotencyKeyStore that delegates all operations to a real store
     * but allows controlling the healthCheck() return value.
     */
    private static class DelegatingKeyStore implements IdempotencyKeyStore {

        private final IdempotencyKeyStore delegate;
        private final AtomicBoolean healthy;

        DelegatingKeyStore(IdempotencyKeyStore delegate, AtomicBoolean healthy) {
            this.delegate = delegate;
            this.healthy = healthy;
        }

        @Override
        public boolean contains(String key) {
            return delegate.contains(key);
        }

        @Override
        public void put(String key, Instant processedAt) {
            delegate.put(key, processedAt);
        }

        @Override
        public void evictBefore(Instant cutoff) {
            delegate.evictBefore(cutoff);
        }

        @Override
        public boolean healthCheck() {
            return healthy.get();
        }

        @Override
        public long size() {
            return delegate.size();
        }
    }
}
