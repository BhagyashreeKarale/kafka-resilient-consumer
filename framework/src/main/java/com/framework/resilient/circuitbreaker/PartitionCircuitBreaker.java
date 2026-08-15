package com.framework.resilient.circuitbreaker;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Per-partition circuit breaker state machine controlling partition isolation via Kafka pause/resume.
 *
 * <p>State transitions:
 * <ul>
 *   <li>CLOSED → OPEN: on degradation signal (error rate or latency threshold exceeded)</li>
 *   <li>OPEN → HALF_OPEN: after cooldown period elapses</li>
 *   <li>HALF_OPEN → CLOSED: on probe batch success</li>
 *   <li>HALF_OPEN → OPEN: on probe batch failure or timeout</li>
 * </ul>
 *
 * <p>This class is designed to be called from the single-threaded poll loop for state transitions,
 * but uses ConcurrentHashMap for safe reads from metrics/health threads.
 */
public class PartitionCircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(PartitionCircuitBreaker.class);

    private final ConcurrentHashMap<TopicPartition, CircuitBreakerState> states;
    private final ConcurrentHashMap<TopicPartition, Instant> cooldownTimers;
    private final CircuitBreakerProperties properties;
    private final Consumer<TopicPartition> pauseCallback;
    private final Consumer<TopicPartition> resumeCallback;

    /**
     * Constructs a PartitionCircuitBreaker with the given configuration and callbacks.
     *
     * @param properties     circuit breaker configuration (thresholds, cooldown, probe settings)
     * @param pauseCallback  callback invoked to pause a partition (delegates to Kafka consumer pause)
     * @param resumeCallback callback invoked to resume a partition (delegates to Kafka consumer resume)
     */
    public PartitionCircuitBreaker(CircuitBreakerProperties properties,
                                   Consumer<TopicPartition> pauseCallback,
                                   Consumer<TopicPartition> resumeCallback) {
        this.properties = properties;
        this.pauseCallback = pauseCallback;
        this.resumeCallback = resumeCallback;
        this.states = new ConcurrentHashMap<>();
        this.cooldownTimers = new ConcurrentHashMap<>();
    }

    // --- State queries ---

    /**
     * Returns the current circuit breaker state for the given partition.
     *
     * @param partition the Kafka partition to query
     * @return the current CircuitState, or CLOSED if the partition is not tracked
     */
    public CircuitState getState(TopicPartition partition) {
        CircuitBreakerState state = states.get(partition);
        return state != null ? state.state() : CircuitState.CLOSED;
    }

    /**
     * Returns whether the partition is healthy (CLOSED state) and can process events.
     *
     * @param partition the Kafka partition to check
     * @return true if the partition is in CLOSED or HALF_OPEN state (can process events)
     */
    public boolean isPartitionHealthy(TopicPartition partition) {
        CircuitState state = getState(partition);
        return state == CircuitState.CLOSED || state == CircuitState.HALF_OPEN;
    }

    // --- State transitions ---

    /**
     * Handles a degradation signal from the health monitor.
     * Transitions the partition from CLOSED to OPEN and invokes the pause callback.
     *
     * <p>Only triggers a transition if the partition is currently in CLOSED state.
     * If already OPEN or HALF_OPEN, the signal is ignored.
     *
     * @param partition the affected partition
     * @param signal    the degradation signal with details about the threshold breach
     */
    public void onDegradationSignal(TopicPartition partition, DegradationSignal signal) {
        CircuitBreakerState currentState = states.get(partition);
        if (currentState == null) {
            log.warn("Received degradation signal for untracked partition: {}", partition);
            return;
        }

        if (currentState.state() != CircuitState.CLOSED) {
            log.debug("Ignoring degradation signal for partition {} already in {} state",
                    partition, currentState.state());
            return;
        }

        // Transition CLOSED → OPEN
        CircuitBreakerState newState = currentState.transitionTo(CircuitState.OPEN);
        states.put(partition, newState);
        cooldownTimers.put(partition, Instant.now());

        log.info("Circuit breaker CLOSED → OPEN for partition {} due to {} (value={}, threshold={})",
                partition, signal.type(), signal.currentValue(), signal.threshold());

        pauseCallback.accept(partition);
    }

    /**
     * Evaluates cooldown timers for all OPEN partitions.
     * Called periodically from the poll loop to check if any partition's cooldown has expired.
     *
     * <p>For each partition whose cooldown has expired, transitions from OPEN to HALF_OPEN
     * and invokes the resume callback to allow probe events to flow.
     */
    public void evaluateCooldowns() {
        for (Map.Entry<TopicPartition, CircuitBreakerState> entry : states.entrySet()) {
            TopicPartition partition = entry.getKey();
            CircuitBreakerState state = entry.getValue();

            if (state.isCooldownExpired(properties.cooldownPeriod())) {
                // Transition OPEN → HALF_OPEN
                CircuitBreakerState newState = state.transitionTo(CircuitState.HALF_OPEN);
                states.put(partition, newState);
                cooldownTimers.remove(partition);

                log.info("Circuit breaker OPEN → HALF_OPEN for partition {} (cooldown expired)",
                        partition);

                resumeCallback.accept(partition);
            }
        }
    }

    /**
     * Handles the result of a probe batch during the HALF_OPEN state.
     *
     * <p>Evaluation logic:
     * <ul>
     *   <li>If the probe timed out with no events: treat as successful (zero available events = success)</li>
     *   <li>If any failure occurred or the probe timed out with failures: transition back to OPEN</li>
     *   <li>If fewer than probeBatchSize events but at least 1 succeeded with no failures: transition to CLOSED</li>
     *   <li>If all events succeeded: transition to CLOSED</li>
     * </ul>
     *
     * @param partition the partition being probed
     * @param result    the probe batch result containing success/failure counts
     */
    public void onProbeResult(TopicPartition partition, ProbeResult result) {
        CircuitBreakerState currentState = states.get(partition);
        if (currentState == null) {
            log.warn("Received probe result for untracked partition: {}", partition);
            return;
        }

        if (currentState.state() != CircuitState.HALF_OPEN) {
            log.debug("Ignoring probe result for partition {} not in HALF_OPEN state (current: {})",
                    partition, currentState.state());
            return;
        }

        boolean probeSuccessful = evaluateProbeSuccess(result);

        if (probeSuccessful) {
            // Transition HALF_OPEN → CLOSED
            CircuitBreakerState newState = currentState.transitionTo(CircuitState.CLOSED);
            states.put(partition, newState);

            log.info("Circuit breaker HALF_OPEN → CLOSED for partition {} (probe succeeded: total={}, success={}, failed={})",
                    partition, result.totalEvents(), result.successCount(), result.failureCount());
        } else {
            // Transition HALF_OPEN → OPEN
            CircuitBreakerState newState = currentState.transitionTo(CircuitState.OPEN);
            states.put(partition, newState);
            cooldownTimers.put(partition, Instant.now());

            log.info("Circuit breaker HALF_OPEN → OPEN for partition {} (probe failed: total={}, success={}, failed={}, timedOut={})",
                    partition, result.totalEvents(), result.successCount(), result.failureCount(), result.timedOut());

            pauseCallback.accept(partition);
        }
    }

    // --- Lifecycle ---

    /**
     * Initializes a newly assigned partition in the CLOSED state.
     *
     * @param partition the partition to initialize
     */
    public void initializePartition(TopicPartition partition) {
        states.put(partition, CircuitBreakerState.initial());
        log.debug("Initialized circuit breaker for partition {} in CLOSED state", partition);
    }

    /**
     * Removes a partition from tracking (typically on partition revocation).
     *
     * @param partition the partition to remove
     */
    public void removePartition(TopicPartition partition) {
        states.remove(partition);
        cooldownTimers.remove(partition);
        log.debug("Removed circuit breaker state for partition {}", partition);
    }

    // --- State persistence ---

    /**
     * Creates a snapshot of all partition circuit breaker states for persistence.
     *
     * @return map of partition to snapshot for each tracked partition
     */
    public Map<TopicPartition, CircuitBreakerSnapshot> snapshot() {
        return states.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> toSnapshot(entry.getValue())
                ));
    }

    /**
     * Restores circuit breaker states from previously persisted snapshots.
     * For partitions restored in OPEN state, re-establishes the cooldown timer
     * based on the original state entry time.
     *
     * @param snapshots map of partition to snapshot to restore
     */
    public void restore(Map<TopicPartition, CircuitBreakerSnapshot> snapshots) {
        for (Map.Entry<TopicPartition, CircuitBreakerSnapshot> entry : snapshots.entrySet()) {
            TopicPartition partition = entry.getKey();
            CircuitBreakerSnapshot snapshot = entry.getValue();

            CircuitBreakerState state = fromSnapshot(snapshot);
            states.put(partition, state);

            // Re-establish cooldown timer for OPEN partitions
            if (state.state() == CircuitState.OPEN) {
                cooldownTimers.put(partition, state.stateEnteredAt());
            }

            log.debug("Restored circuit breaker state for partition {}: state={}, enteredAt={}",
                    partition, snapshot.state(), snapshot.stateEnteredAt());
        }
    }

    // --- Internal helpers ---

    /**
     * Evaluates whether a probe result indicates successful recovery.
     *
     * <p>Logic per design:
     * <ul>
     *   <li>Zero available events within probe timeout = successful probe</li>
     *   <li>If fewer than probeBatchSize events but at least 1 succeeded with no failures = success</li>
     *   <li>If any failure or timeout with partial processing = failure</li>
     * </ul>
     */
    private boolean evaluateProbeSuccess(ProbeResult result) {
        // Zero events within timeout = successful probe (no events available = healthy)
        if (result.totalEvents() == 0 && !result.timedOut()) {
            return true;
        }

        // Timed out with zero events consumed is also treated as success
        if (result.totalEvents() == 0 && result.timedOut()) {
            return true;
        }

        // Any failure = probe failed
        if (result.failureCount() > 0) {
            return false;
        }

        // Timed out with some events still unprocessed = probe failed
        if (result.timedOut()) {
            return false;
        }

        // All processed events succeeded (at least 1)
        return result.successCount() > 0;
    }

    private CircuitBreakerSnapshot toSnapshot(CircuitBreakerState state) {
        return new CircuitBreakerSnapshot(
                state.state(),
                state.stateEnteredAt(),
                state.consecutiveFailures(),
                state.lastTransitionTime()
        );
    }

    private CircuitBreakerState fromSnapshot(CircuitBreakerSnapshot snapshot) {
        return new CircuitBreakerState(
                snapshot.state(),
                snapshot.stateEnteredAt(),
                snapshot.lastTransitionTime(),
                snapshot.consecutiveFailures(),
                0,  // probe counters reset on restore
                0
        );
    }
}
