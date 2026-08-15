package com.framework.resilient.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot;
import com.framework.resilient.circuitbreaker.CircuitState;
import com.framework.resilient.dedup.DeduplicationSnapshot;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class FileBasedStateStoreTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);

    @TempDir
    Path tempDir;

    private ObjectMapper objectMapper;
    private FileBasedStateStore stateStore;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        stateStore = new FileBasedStateStore(tempDir, objectMapper);
    }

    // --- Persist and restore round-trip for circuit breaker state ---

    @Test
    void persistAndRestore_circuitBreakerState_shouldRoundTrip() {
        Instant now = Instant.now();
        CircuitBreakerSnapshot snapshot = new CircuitBreakerSnapshot(
                CircuitState.OPEN, now, 5, now
        );

        stateStore.persistCircuitBreakerState(Map.of(PARTITION_0, snapshot));

        Optional<CircuitBreakerSnapshot> restored = stateStore.restoreCircuitBreakerState(PARTITION_0);

        assertThat(restored).isPresent();
        assertThat(restored.get().state()).isEqualTo(CircuitState.OPEN);
        assertThat(restored.get().consecutiveFailures()).isEqualTo(5);
    }

    @Test
    void persistAndRestore_circuitBreakerState_closedState() {
        Instant now = Instant.now();
        CircuitBreakerSnapshot snapshot = new CircuitBreakerSnapshot(
                CircuitState.CLOSED, now, 0, now
        );

        stateStore.persistCircuitBreakerState(Map.of(PARTITION_0, snapshot));

        Optional<CircuitBreakerSnapshot> restored = stateStore.restoreCircuitBreakerState(PARTITION_0);

        assertThat(restored).isPresent();
        assertThat(restored.get().state()).isEqualTo(CircuitState.CLOSED);
        assertThat(restored.get().consecutiveFailures()).isEqualTo(0);
    }

    @Test
    void persistAndRestore_circuitBreakerState_multiplePartitions() {
        Instant now = Instant.now();
        CircuitBreakerSnapshot snapshot0 = new CircuitBreakerSnapshot(CircuitState.OPEN, now, 3, now);
        CircuitBreakerSnapshot snapshot1 = new CircuitBreakerSnapshot(CircuitState.HALF_OPEN, now, 1, now);

        stateStore.persistCircuitBreakerState(Map.of(PARTITION_0, snapshot0, PARTITION_1, snapshot1));

        assertThat(stateStore.restoreCircuitBreakerState(PARTITION_0).get().state())
                .isEqualTo(CircuitState.OPEN);
        assertThat(stateStore.restoreCircuitBreakerState(PARTITION_1).get().state())
                .isEqualTo(CircuitState.HALF_OPEN);
    }

    // --- Persist and restore round-trip for dedup state ---

    @Test
    void persistAndRestore_deduplicationState_shouldRoundTrip() {
        Instant now = Instant.now();
        Map<String, Instant> keys = Map.of(
                "entity-1:100:0", now,
                "entity-2:200:0", now.minusSeconds(60)
        );
        DeduplicationSnapshot snapshot = new DeduplicationSnapshot(keys);

        stateStore.persistDeduplicationState(Map.of(PARTITION_0, snapshot));

        Optional<DeduplicationSnapshot> restored = stateStore.restoreDeduplicationState(PARTITION_0);

        assertThat(restored).isPresent();
        assertThat(restored.get().keys()).containsKey("entity-1:100:0");
        assertThat(restored.get().keys()).containsKey("entity-2:200:0");
        assertThat(restored.get().keys()).hasSize(2);
    }

    @Test
    void persistAndRestore_deduplicationState_emptyKeys() {
        DeduplicationSnapshot snapshot = new DeduplicationSnapshot(Map.of());

        stateStore.persistDeduplicationState(Map.of(PARTITION_0, snapshot));

        Optional<DeduplicationSnapshot> restored = stateStore.restoreDeduplicationState(PARTITION_0);

        assertThat(restored).isPresent();
        assertThat(restored.get().keys()).isEmpty();
    }

    // --- Restoring non-existent state returns Optional.empty() ---

    @Test
    void restoreCircuitBreakerState_forNonExistentPartition_shouldReturnEmpty() {
        Optional<CircuitBreakerSnapshot> restored = stateStore.restoreCircuitBreakerState(PARTITION_0);

        assertThat(restored).isEmpty();
    }

    @Test
    void restoreDeduplicationState_forNonExistentPartition_shouldReturnEmpty() {
        Optional<DeduplicationSnapshot> restored = stateStore.restoreDeduplicationState(PARTITION_0);

        assertThat(restored).isEmpty();
    }

    @Test
    void restoreBufferState_forNonExistentPartition_shouldReturnEmpty() {
        Optional<?> restored = stateStore.restoreBufferState(PARTITION_0);

        assertThat(restored).isEmpty();
    }

    // --- removeState cleans up files ---

    @Test
    void removeState_shouldDeleteAllStateFiles() {
        Instant now = Instant.now();
        stateStore.persistCircuitBreakerState(Map.of(PARTITION_0,
                new CircuitBreakerSnapshot(CircuitState.OPEN, now, 1, now)));
        stateStore.persistDeduplicationState(Map.of(PARTITION_0,
                new DeduplicationSnapshot(Map.of("key:1:0", now))));

        // Verify files exist before removal
        assertThat(stateStore.restoreCircuitBreakerState(PARTITION_0)).isPresent();
        assertThat(stateStore.restoreDeduplicationState(PARTITION_0)).isPresent();

        stateStore.removeState(PARTITION_0);

        assertThat(stateStore.restoreCircuitBreakerState(PARTITION_0)).isEmpty();
        assertThat(stateStore.restoreDeduplicationState(PARTITION_0)).isEmpty();
    }

    @Test
    void removeState_forNonExistentPartition_shouldNotThrow() {
        // Should not throw when removing state for a partition that has no persisted files
        stateStore.removeState(PARTITION_0);
    }

    @Test
    void removeState_shouldNotAffectOtherPartitions() {
        Instant now = Instant.now();
        stateStore.persistCircuitBreakerState(Map.of(
                PARTITION_0, new CircuitBreakerSnapshot(CircuitState.OPEN, now, 1, now),
                PARTITION_1, new CircuitBreakerSnapshot(CircuitState.CLOSED, now, 0, now)
        ));

        stateStore.removeState(PARTITION_0);

        assertThat(stateStore.restoreCircuitBreakerState(PARTITION_0)).isEmpty();
        assertThat(stateStore.restoreCircuitBreakerState(PARTITION_1)).isPresent();
    }

    // --- Corrupted file returns Optional.empty() ---

    @Test
    void restoreCircuitBreakerState_withCorruptedFile_shouldReturnEmpty() throws IOException {
        // Write garbage to the expected file location
        Path corruptedFile = tempDir.resolve("cb-test-topic-0.json");
        Files.writeString(corruptedFile, "this is not valid json {{{corrupt data!!!");

        Optional<CircuitBreakerSnapshot> restored = stateStore.restoreCircuitBreakerState(PARTITION_0);

        assertThat(restored).isEmpty();
    }

    @Test
    void restoreDeduplicationState_withCorruptedFile_shouldReturnEmpty() throws IOException {
        Path corruptedFile = tempDir.resolve("dedup-test-topic-0.json");
        Files.writeString(corruptedFile, "random garbage content \u0000\u0001\u0002");

        Optional<DeduplicationSnapshot> restored = stateStore.restoreDeduplicationState(PARTITION_0);

        assertThat(restored).isEmpty();
    }

    @Test
    void restoreBufferState_withCorruptedFile_shouldReturnEmpty() throws IOException {
        Path corruptedFile = tempDir.resolve("rb-test-topic-0.json");
        Files.writeString(corruptedFile, "{{{{not json at all}}}}");

        Optional<?> restored = stateStore.restoreBufferState(PARTITION_0);

        assertThat(restored).isEmpty();
    }

    // --- isAvailable() returns true when directory exists ---

    @Test
    void isAvailable_withExistingDirectory_shouldReturnTrue() {
        assertThat(stateStore.isAvailable()).isTrue();
    }

    @Test
    void isAvailable_withNonExistentDirectory_shouldReturnFalse() {
        Path nonExistent = tempDir.resolve("does-not-exist");
        FileBasedStateStore unavailableStore = new FileBasedStateStore(nonExistent, objectMapper);

        assertThat(unavailableStore.isAvailable()).isFalse();
    }
}
