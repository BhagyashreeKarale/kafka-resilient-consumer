package com.framework.resilient.circuitbreaker;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionCircuitBreakerTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);

    private List<TopicPartition> pausedPartitions;
    private List<TopicPartition> resumedPartitions;
    private CircuitBreakerProperties properties;
    private PartitionCircuitBreaker breaker;

    @BeforeEach
    void setUp() {
        pausedPartitions = new ArrayList<>();
        resumedPartitions = new ArrayList<>();

        // Test-friendly properties with short cooldown (100ms)
        properties = new CircuitBreakerProperties(
                0.5,
                Duration.ofMillis(5000),
                Duration.ofMillis(100),  // short cooldown for testing
                10,
                Duration.ofSeconds(30),
                Duration.ofSeconds(2),
                Duration.ofSeconds(1)
        );

        breaker = new PartitionCircuitBreaker(
                properties,
                pausedPartitions::add,
                resumedPartitions::add
        );
    }

    // --- Test 1: Initialization always starts in CLOSED state ---

    @Test
    void initializePartition_shouldStartInClosedState() {
        breaker.initializePartition(PARTITION_0);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
        assertThat(breaker.isPartitionHealthy(PARTITION_0)).isTrue();
    }

    @Test
    void getState_forUntrackedPartition_shouldReturnClosed() {
        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
    }

    // --- Test 2: CLOSED → OPEN transition on degradation signal ---

    @Test
    void onDegradationSignal_whenClosed_shouldTransitionToOpen() {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.75,
                0.5,
                Instant.now()
        );

        breaker.onDegradationSignal(PARTITION_0, signal);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
        assertThat(breaker.isPartitionHealthy(PARTITION_0)).isFalse();
    }

    @Test
    void onDegradationSignal_whenClosed_shouldInvokePauseCallback() {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.LATENCY,
                6000.0,
                5000.0,
                Instant.now()
        );

        breaker.onDegradationSignal(PARTITION_0, signal);

        assertThat(pausedPartitions).containsExactly(PARTITION_0);
        assertThat(resumedPartitions).isEmpty();
    }

    // --- Test 3: Ignores degradation signal when already in OPEN state ---

    @Test
    void onDegradationSignal_whenAlreadyOpen_shouldBeIgnored() {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );

        // First signal transitions to OPEN
        breaker.onDegradationSignal(PARTITION_0, signal);
        // Second signal should be ignored
        breaker.onDegradationSignal(PARTITION_0, signal);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
        // Pause callback should only be called once
        assertThat(pausedPartitions).hasSize(1);
    }

    @Test
    void onDegradationSignal_forUntrackedPartition_shouldBeIgnored() {
        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );

        breaker.onDegradationSignal(PARTITION_0, signal);

        assertThat(pausedPartitions).isEmpty();
    }

    // --- Test 4: OPEN → HALF_OPEN transition after cooldown period expires ---

    @Test
    void evaluateCooldowns_afterCooldownExpires_shouldTransitionToHalfOpen() throws InterruptedException {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(PARTITION_0, signal);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);

        // Wait for cooldown to expire (100ms + buffer)
        Thread.sleep(150);

        breaker.evaluateCooldowns();

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.HALF_OPEN);
        assertThat(breaker.isPartitionHealthy(PARTITION_0)).isTrue();
    }

    @Test
    void evaluateCooldowns_afterCooldownExpires_shouldInvokeResumeCallback() throws InterruptedException {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(PARTITION_0, signal);

        Thread.sleep(150);

        breaker.evaluateCooldowns();

        assertThat(resumedPartitions).containsExactly(PARTITION_0);
    }

    @Test
    void evaluateCooldowns_beforeCooldownExpires_shouldNotTransition() {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(PARTITION_0, signal);

        // Evaluate immediately — cooldown has not expired
        breaker.evaluateCooldowns();

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
        assertThat(resumedPartitions).isEmpty();
    }

    // --- Test 5: HALF_OPEN → CLOSED on probe success ---

    @Test
    void onProbeResult_allEventsSucceed_shouldTransitionToClosed() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);

        ProbeResult result = new ProbeResult(10, 10, 0, false);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
        assertThat(breaker.isPartitionHealthy(PARTITION_0)).isTrue();
    }

    @Test
    void onProbeResult_partialSuccessNoFailures_shouldTransitionToClosed() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);

        // Fewer than probeBatchSize but all succeeded, no timeout
        ProbeResult result = new ProbeResult(5, 5, 0, false);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void onProbeResult_success_shouldNotInvokePauseCallback() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);
        pausedPartitions.clear(); // Clear the pause from CLOSED→OPEN

        ProbeResult result = new ProbeResult(10, 10, 0, false);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(pausedPartitions).isEmpty();
    }

    // --- Test 6: HALF_OPEN → OPEN on probe failure ---

    @Test
    void onProbeResult_anyEventFails_shouldTransitionToOpen() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);

        ProbeResult result = new ProbeResult(10, 9, 1, false);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
    }

    @Test
    void onProbeResult_failure_shouldInvokePauseCallback() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);
        pausedPartitions.clear(); // Clear the pause from CLOSED→OPEN

        ProbeResult result = new ProbeResult(10, 8, 2, false);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(pausedPartitions).containsExactly(PARTITION_0);
    }

    // --- Test 7: HALF_OPEN → OPEN on probe timeout ---

    @Test
    void onProbeResult_timedOutWithEvents_shouldTransitionToOpen() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);

        // Timed out with some events processed but not all — treated as failure
        ProbeResult result = new ProbeResult(5, 5, 0, true);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
    }

    @Test
    void onProbeResult_timedOutWithFailures_shouldTransitionToOpen() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);

        ProbeResult result = new ProbeResult(5, 3, 2, true);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
    }

    // --- Test 8: Zero events with timeout treated as successful probe ---

    @Test
    void onProbeResult_zeroEventsTimedOut_shouldTransitionToClosed() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);

        // Zero events within timeout = healthy (no events available)
        ProbeResult result = new ProbeResult(0, 0, 0, true);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void onProbeResult_zeroEventsNoTimeout_shouldTransitionToClosed() throws InterruptedException {
        transitionToHalfOpen(PARTITION_0);

        ProbeResult result = new ProbeResult(0, 0, 0, false);
        breaker.onProbeResult(PARTITION_0, result);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
    }

    // --- Test 9: Independent partition states ---

    @Test
    void partitionsHaveIndependentStates() {
        breaker.initializePartition(PARTITION_0);
        breaker.initializePartition(PARTITION_1);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );

        // Only transition partition 0
        breaker.onDegradationSignal(PARTITION_0, signal);

        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
        assertThat(breaker.getState(PARTITION_1)).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void partitionTransition_doesNotAffectOtherPartitions() throws InterruptedException {
        breaker.initializePartition(PARTITION_0);
        breaker.initializePartition(PARTITION_1);

        // Transition partition 0 through full cycle
        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(PARTITION_0, signal);

        Thread.sleep(150);
        breaker.evaluateCooldowns();

        ProbeResult result = new ProbeResult(10, 10, 0, false);
        breaker.onProbeResult(PARTITION_0, result);

        // Partition 0 went CLOSED → OPEN → HALF_OPEN → CLOSED
        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
        // Partition 1 stayed CLOSED the entire time
        assertThat(breaker.getState(PARTITION_1)).isEqualTo(CircuitState.CLOSED);
        // Pause/resume only affected partition 0
        assertThat(pausedPartitions).containsExactly(PARTITION_0);
        assertThat(resumedPartitions).containsExactly(PARTITION_0);
    }

    // --- Test 10: Snapshot and restore round-trip preserves state ---

    @Test
    void snapshotAndRestore_preservesClosedState() {
        breaker.initializePartition(PARTITION_0);

        Map<TopicPartition, CircuitBreakerSnapshot> snapshot = breaker.snapshot();

        // Create a new breaker and restore
        PartitionCircuitBreaker restoredBreaker = new PartitionCircuitBreaker(
                properties, pausedPartitions::add, resumedPartitions::add
        );
        restoredBreaker.restore(snapshot);

        assertThat(restoredBreaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void snapshotAndRestore_preservesOpenState() {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(PARTITION_0, signal);

        Map<TopicPartition, CircuitBreakerSnapshot> snapshot = breaker.snapshot();

        PartitionCircuitBreaker restoredBreaker = new PartitionCircuitBreaker(
                properties, pausedPartitions::add, resumedPartitions::add
        );
        restoredBreaker.restore(snapshot);

        assertThat(restoredBreaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
    }

    @Test
    void snapshotAndRestore_preservesMultiplePartitions() {
        breaker.initializePartition(PARTITION_0);
        breaker.initializePartition(PARTITION_1);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(PARTITION_0, signal);

        Map<TopicPartition, CircuitBreakerSnapshot> snapshot = breaker.snapshot();

        assertThat(snapshot).hasSize(2);
        assertThat(snapshot.get(PARTITION_0).state()).isEqualTo(CircuitState.OPEN);
        assertThat(snapshot.get(PARTITION_1).state()).isEqualTo(CircuitState.CLOSED);

        PartitionCircuitBreaker restoredBreaker = new PartitionCircuitBreaker(
                properties, pausedPartitions::add, resumedPartitions::add
        );
        restoredBreaker.restore(snapshot);

        assertThat(restoredBreaker.getState(PARTITION_0)).isEqualTo(CircuitState.OPEN);
        assertThat(restoredBreaker.getState(PARTITION_1)).isEqualTo(CircuitState.CLOSED);
    }

    // --- Test 11: removePartition cleans up state ---

    @Test
    void removePartition_shouldRemoveTrackedState() {
        breaker.initializePartition(PARTITION_0);
        breaker.initializePartition(PARTITION_1);

        breaker.removePartition(PARTITION_0);

        // Removed partition returns default CLOSED (untracked)
        assertThat(breaker.getState(PARTITION_0)).isEqualTo(CircuitState.CLOSED);
        // Other partition still tracked
        assertThat(breaker.getState(PARTITION_1)).isEqualTo(CircuitState.CLOSED);
    }

    @Test
    void removePartition_shouldCleanUpCooldownTimers() throws InterruptedException {
        breaker.initializePartition(PARTITION_0);

        DegradationSignal signal = new DegradationSignal(
                PARTITION_0,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(PARTITION_0, signal);

        // Remove partition while in OPEN state (cooldown timer should be cleaned)
        breaker.removePartition(PARTITION_0);

        Thread.sleep(150);
        breaker.evaluateCooldowns();

        // Should not trigger resume for removed partition
        assertThat(resumedPartitions).isEmpty();
    }

    @Test
    void removePartition_snapshotShouldNotContainRemovedPartition() {
        breaker.initializePartition(PARTITION_0);
        breaker.initializePartition(PARTITION_1);

        breaker.removePartition(PARTITION_0);

        Map<TopicPartition, CircuitBreakerSnapshot> snapshot = breaker.snapshot();
        assertThat(snapshot).containsOnlyKeys(PARTITION_1);
    }

    // --- Helper methods ---

    /**
     * Transitions the given partition from CLOSED → OPEN → HALF_OPEN
     * by sending a degradation signal and waiting for the cooldown to expire.
     */
    private void transitionToHalfOpen(TopicPartition partition) throws InterruptedException {
        breaker.initializePartition(partition);

        DegradationSignal signal = new DegradationSignal(
                partition,
                DegradationType.ERROR_RATE,
                0.8,
                0.5,
                Instant.now()
        );
        breaker.onDegradationSignal(partition, signal);

        Thread.sleep(150); // Wait for 100ms cooldown to expire
        breaker.evaluateCooldowns();

        assertThat(breaker.getState(partition)).isEqualTo(CircuitState.HALF_OPEN);
    }
}
