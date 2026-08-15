package com.framework.resilient.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.framework.resilient.circuitbreaker.CircuitBreakerSnapshot;
import com.framework.resilient.dedup.DeduplicationSnapshot;
import com.framework.resilient.reorder.BufferSnapshot;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Optional;

/**
 * File-based implementation of {@link StateStore} using Jackson JSON serialization.
 * Persists circuit breaker, reorder buffer, and deduplication state to local JSON files.
 *
 * <p><strong>Deployment scope:</strong> This implementation is intended for single-instance deployments
 * and development/testing environments. It writes state to the local filesystem and provides no
 * coordination between multiple consumer instances. Production deployments requiring shared state
 * across instances should use a distributed store implementation (e.g., Redis, ZooKeeper, or a
 * database-backed {@link StateStore}).
 *
 * <p>Writes are performed atomically (write to temp file, then rename) to minimize the risk of
 * corrupted state files on crash.
 */
public class FileBasedStateStore implements StateStore {

    private static final Logger logger = LoggerFactory.getLogger(FileBasedStateStore.class);

    private final Path baseDirectory;
    private final ObjectMapper objectMapper;

    public FileBasedStateStore(Path baseDirectory, ObjectMapper objectMapper) {
        this.baseDirectory = baseDirectory;
        this.objectMapper = objectMapper;
    }

    @Override
    public void persistCircuitBreakerState(Map<TopicPartition, CircuitBreakerSnapshot> snapshots) {
        for (Map.Entry<TopicPartition, CircuitBreakerSnapshot> entry : snapshots.entrySet()) {
            Path file = resolveCircuitBreakerFile(entry.getKey());
            writeJson(file, entry.getValue(), entry.getKey(), "circuit breaker");
        }
    }

    @Override
    public Optional<CircuitBreakerSnapshot> restoreCircuitBreakerState(TopicPartition partition) {
        Path file = resolveCircuitBreakerFile(partition);
        return readJson(file, CircuitBreakerSnapshot.class, partition, "circuit breaker");
    }

    @Override
    public <T> void persistBufferState(Map<TopicPartition, BufferSnapshot<T>> snapshots) {
        for (Map.Entry<TopicPartition, BufferSnapshot<T>> entry : snapshots.entrySet()) {
            Path file = resolveReorderBufferFile(entry.getKey());
            writeJson(file, entry.getValue(), entry.getKey(), "reorder buffer");
        }
    }

    @Override
    public <T> Optional<BufferSnapshot<T>> restoreBufferState(TopicPartition partition) {
        Path file = resolveReorderBufferFile(partition);
        return readJson(file, BufferSnapshot.class, partition, "reorder buffer")
                .map(snapshot -> {
                    @SuppressWarnings("unchecked")
                    BufferSnapshot<T> typed = (BufferSnapshot<T>) snapshot;
                    return typed;
                });
    }

    @Override
    public void persistDeduplicationState(Map<TopicPartition, DeduplicationSnapshot> snapshots) {
        for (Map.Entry<TopicPartition, DeduplicationSnapshot> entry : snapshots.entrySet()) {
            Path file = resolveDeduplicationFile(entry.getKey());
            writeJson(file, entry.getValue(), entry.getKey(), "deduplication");
        }
    }

    @Override
    public Optional<DeduplicationSnapshot> restoreDeduplicationState(TopicPartition partition) {
        Path file = resolveDeduplicationFile(partition);
        return readJson(file, DeduplicationSnapshot.class, partition, "deduplication");
    }

    @Override
    public void removeState(TopicPartition partition) {
        deleteQuietly(resolveCircuitBreakerFile(partition), partition);
        deleteQuietly(resolveReorderBufferFile(partition), partition);
        deleteQuietly(resolveDeduplicationFile(partition), partition);
    }

    @Override
    public boolean isAvailable() {
        return Files.exists(baseDirectory) && Files.isWritable(baseDirectory);
    }

    // --- Private helpers ---

    private Path resolveCircuitBreakerFile(TopicPartition partition) {
        return baseDirectory.resolve(
                String.format("cb-%s-%d.json", partition.topic(), partition.partition()));
    }

    private Path resolveReorderBufferFile(TopicPartition partition) {
        return baseDirectory.resolve(
                String.format("rb-%s-%d.json", partition.topic(), partition.partition()));
    }

    private Path resolveDeduplicationFile(TopicPartition partition) {
        return baseDirectory.resolve(
                String.format("dedup-%s-%d.json", partition.topic(), partition.partition()));
    }

    private void writeJson(Path file, Object value, TopicPartition partition, String stateType) {
        try {
            Files.createDirectories(file.getParent());
            String json = objectMapper.writeValueAsString(value);
            Path tempFile = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tempFile, json);
            try {
                Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                // Fall back to non-atomic replace if filesystem doesn't support atomic move
                Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            logger.error("Failed to persist {} state for partition {}: {}",
                    stateType, partition, e.getMessage(), e);
            throw new RuntimeException(
                    String.format("Failed to persist %s state for partition %s", stateType, partition), e);
        }
    }

    private <T> Optional<T> readJson(Path file, Class<T> type, TopicPartition partition, String stateType) {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            T value = objectMapper.readValue(file.toFile(), type);
            return Optional.of(value);
        } catch (IOException e) {
            logger.warn("Failed to restore {} state for partition {} (file may be corrupted): {}",
                    stateType, partition, e.getMessage(), e);
            return Optional.empty();
        }
    }

    private void deleteQuietly(Path file, TopicPartition partition) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            logger.warn("Failed to delete state file {} for partition {}: {}",
                    file, partition, e.getMessage(), e);
        }
    }
}
