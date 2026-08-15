package com.framework.resilient.circuitbreaker;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link PartitionHealthMonitor}.
 *
 * <p>Uses short sliding window (5 seconds) and low thresholds for fast, deterministic tests.
 */
class PartitionHealthMonitorTest {

    private static final TopicPartition PARTITION_0 = new TopicPartition("test-topic", 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition("test-topic", 1);

    private static final Duration SHORT_LATENCY = Duration.ofMillis(10);
    private static final Duration LONG_LATENCY = Duration.ofMillis(6000);

    private HealthMonitorProperties healthProperties;
    private CircuitBreakerProperties circuitBreakerProperties;
    private List<DegradationSignal> emittedSignals;
    private PartitionHealthMonitor monitor;

    @BeforeEach
    void setUp() {
        // Short 5-second window for testing
        healthProperties = new HealthMonitorProperties(
                Duration.ofSeconds(5),
                Duration.ofSeconds(1)
        );
        // Error rate threshold 0.5, latency threshold 5000ms
        circuitBreakerProperties = new CircuitBreakerProperties(
                0.5,
                Duration.ofMillis(5000),
                Duration.ofSeconds(30),
                10,
                Duration.ofSeconds(30),
                Duration.ofSeconds(2),
                Duration.ofSeconds(1)
        );
        emittedSignals = Collections.synchronizedList(new ArrayList<>());
        monitor = new PartitionHealthMonitor(healthProperties, circuitBreakerProperties, emittedSignals::add);
        monitor.initializePartition(PARTITION_0);
    }

    @Nested
    @DisplayName("recordSuccess() and recordFailure()")
    class MetricRecording {

        @Test
        @DisplayName("recordSuccess updates per-partition metrics")
        void recordSuccess_updatesPartitionMetrics() {
            monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);

            assertThat(monitor.getErrorRate(PARTITION_0)).isEqualTo(0.0);
        }

        @Test
        @DisplayName("recordFailure updates per-partition metrics")
        void recordFailure_updatesPartitionMetrics() {
            monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            monitor.recordFailure(PARTITION_0, SHORT_LATENCY);

            assertThat(monitor.getErrorRate(PARTITION_0)).isEqualTo(1.0);
        }

        @Test
        @DisplayName("records are isolated per partition")
        void records_areIsolatedPerPartition() {
            monitor.initializePartition(PARTITION_1);

            monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            monitor.recordFailure(PARTITION_1, SHORT_LATENCY);

            assertThat(monitor.getErrorRate(PARTITION_0)).isEqualTo(0.0);
            assertThat(monitor.getErrorRate(PARTITION_1)).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("getErrorRate()")
    class ErrorRate {

        @Test
        @DisplayName("returns correct ratio of failures to total")
        void returnsCorrectRatio() {
            // 3 successes + 2 failures = 2/5 = 0.4
            monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            monitor.recordFailure(PARTITION_0, SHORT_LATENCY);

            double errorRate = monitor.getErrorRate(PARTITION_0);

            assertThat(errorRate).isCloseTo(0.4, within(0.001));
        }

        @Test
        @DisplayName("returns 0.0 when no events recorded")
        void returnsZero_whenNoEvents() {
            assertThat(monitor.getErrorRate(PARTITION_0)).isEqualTo(0.0);
        }

        @Test
        @DisplayName("returns 0.0 for untracked partition")
        void returnsZero_forUntrackedPartition() {
            TopicPartition untracked = new TopicPartition("unknown", 99);
            assertThat(monitor.getErrorRate(untracked)).isEqualTo(0.0);
        }
    }

    @Nested
    @DisplayName("getLatencyPercentiles()")
    class LatencyPercentiles_ {

        @Test
        @DisplayName("returns p50, p95, p99 values based on recorded latencies")
        void returnsPercentileValues() {
            // Record 100 events with increasing latencies from 1ms to 100ms
            for (int i = 1; i <= 100; i++) {
                monitor.recordSuccess(PARTITION_0, Duration.ofMillis(i));
            }

            LatencyPercentiles percentiles = monitor.getLatencyPercentiles(PARTITION_0);

            // p50 ~ 50ms, p95 ~ 95ms, p99 ~ 99ms
            assertThat(percentiles.p50()).isBetween(Duration.ofMillis(45), Duration.ofMillis(55));
            assertThat(percentiles.p95()).isBetween(Duration.ofMillis(90), Duration.ofMillis(100));
            assertThat(percentiles.p99()).isBetween(Duration.ofMillis(95), Duration.ofMillis(100));
        }

        @Test
        @DisplayName("returns zero durations when no data recorded")
        void returnsZeroDurations_whenNoData() {
            LatencyPercentiles percentiles = monitor.getLatencyPercentiles(PARTITION_0);

            assertThat(percentiles.p50()).isEqualTo(Duration.ZERO);
            assertThat(percentiles.p95()).isEqualTo(Duration.ZERO);
            assertThat(percentiles.p99()).isEqualTo(Duration.ZERO);
        }
    }

    @Nested
    @DisplayName("evaluate() - degradation signal emission")
    class Evaluate {

        @Test
        @DisplayName("emits degradation signal when error rate exceeds threshold")
        void emitsSignal_whenErrorRateExceedsThreshold() {
            // Push error rate above 0.5 threshold: 7 failures / 10 total = 0.7
            for (int i = 0; i < 3; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            for (int i = 0; i < 7; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }

            monitor.evaluate(PARTITION_0);

            assertThat(emittedSignals).hasSize(1);
            DegradationSignal signal = emittedSignals.get(0);
            assertThat(signal.partition()).isEqualTo(PARTITION_0);
            assertThat(signal.type()).isEqualTo(DegradationType.ERROR_RATE);
            assertThat(signal.currentValue()).isCloseTo(0.7, within(0.001));
            assertThat(signal.threshold()).isEqualTo(0.5);
        }

        @Test
        @DisplayName("emits degradation signal when p99 latency exceeds threshold")
        void emitsSignal_whenP99LatencyExceedsThreshold() {
            // Record latencies that push p99 above 5000ms threshold.
            // With 100 events, p99 index = ceil(0.99 * 100) - 1 = 98.
            // We need at least 2 slow events so index 98 is a slow one.
            for (int i = 0; i < 98; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            // Add two very slow events to ensure p99 is above threshold
            monitor.recordSuccess(PARTITION_0, LONG_LATENCY);
            monitor.recordSuccess(PARTITION_0, LONG_LATENCY);

            monitor.evaluate(PARTITION_0);

            assertThat(emittedSignals).anySatisfy(signal -> {
                assertThat(signal.partition()).isEqualTo(PARTITION_0);
                assertThat(signal.type()).isEqualTo(DegradationType.LATENCY);
                assertThat(signal.currentValue()).isGreaterThanOrEqualTo(5000.0);
            });
        }

        @Test
        @DisplayName("does NOT emit signal when metrics are within thresholds")
        void doesNotEmitSignal_whenMetricsWithinThresholds() {
            // 2 failures / 10 total = 0.2 (below 0.5 threshold)
            for (int i = 0; i < 8; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            for (int i = 0; i < 2; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }

            monitor.evaluate(PARTITION_0);

            assertThat(emittedSignals).isEmpty();
        }
    }

    @Nested
    @DisplayName("Signal suppression")
    class SignalSuppression {

        @Test
        @DisplayName("after emitting for error rate, does NOT re-emit until metric recovers and re-exceeds")
        void suppressesDuplicateSignals_untilRecoveryAndReExceed() {
            // First: push error rate above threshold and trigger signal
            for (int i = 0; i < 7; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }
            for (int i = 0; i < 3; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1);

            // Second evaluate — still above threshold, should NOT emit again
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1);

            // Third evaluate — still above threshold, should NOT emit again
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1);
        }

        @Test
        @DisplayName("re-emits signal after metric recovers below threshold and then re-exceeds")
        void reEmitsSignal_afterRecoveryAndReExceed() {
            // Push error rate above threshold
            for (int i = 0; i < 7; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }
            for (int i = 0; i < 3; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1);

            // Reset partition to simulate recovery (error rate drops to 0)
            monitor.resetPartition(PARTITION_0);

            // Record events below threshold, evaluate to un-suppress
            for (int i = 0; i < 10; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1); // No new signal, metric is below threshold

            // Now push above threshold again — should emit a new signal
            for (int i = 0; i < 10; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(2); // New signal emitted
        }
    }

    @Nested
    @DisplayName("resetSuppression()")
    class ResetSuppression {

        @Test
        @DisplayName("allows re-emission for a partition after manual suppression reset")
        void allowsReEmission_afterReset() {
            // Push error rate above threshold and trigger signal
            for (int i = 0; i < 8; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }
            for (int i = 0; i < 2; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1);

            // Suppressed — evaluate again, no new signal
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1);

            // Reset suppression manually
            monitor.resetSuppression(PARTITION_0, DegradationType.ERROR_RATE);

            // Now evaluate again — metric still above threshold, should emit again
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(2);
            assertThat(emittedSignals.get(1).type()).isEqualTo(DegradationType.ERROR_RATE);
        }
    }

    @Nested
    @DisplayName("initializePartition()")
    class InitializePartition {

        @Test
        @DisplayName("creates empty state for a new partition")
        void createsEmptyState() {
            TopicPartition newPartition = new TopicPartition("test-topic", 5);
            monitor.initializePartition(newPartition);

            assertThat(monitor.getErrorRate(newPartition)).isEqualTo(0.0);
            LatencyPercentiles percentiles = monitor.getLatencyPercentiles(newPartition);
            assertThat(percentiles.p50()).isEqualTo(Duration.ZERO);
            assertThat(percentiles.p95()).isEqualTo(Duration.ZERO);
            assertThat(percentiles.p99()).isEqualTo(Duration.ZERO);
            assertThat(monitor.getHealth(newPartition)).isNotNull();
            assertThat(monitor.getHealth(newPartition).totalEvents()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("resetPartition()")
    class ResetPartition {

        @Test
        @DisplayName("discards all accumulated window data")
        void discardsAllWindowData() {
            // Record some data
            for (int i = 0; i < 5; i++) {
                monitor.recordSuccess(PARTITION_0, Duration.ofMillis(50));
                monitor.recordFailure(PARTITION_0, Duration.ofMillis(100));
            }
            assertThat(monitor.getErrorRate(PARTITION_0)).isGreaterThan(0.0);

            // Reset partition
            monitor.resetPartition(PARTITION_0);

            // Verify all data is cleared
            assertThat(monitor.getErrorRate(PARTITION_0)).isEqualTo(0.0);
            LatencyPercentiles percentiles = monitor.getLatencyPercentiles(PARTITION_0);
            assertThat(percentiles.p50()).isEqualTo(Duration.ZERO);
            assertThat(percentiles.p95()).isEqualTo(Duration.ZERO);
            assertThat(percentiles.p99()).isEqualTo(Duration.ZERO);
        }

        @Test
        @DisplayName("also clears signal suppression state")
        void clearsSignalSuppressionState() {
            // Trigger a signal and suppress it
            for (int i = 0; i < 8; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }
            for (int i = 0; i < 2; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(1);

            // Reset the partition
            monitor.resetPartition(PARTITION_0);

            // Record new data above threshold — should emit a new signal since suppression was cleared
            for (int i = 0; i < 8; i++) {
                monitor.recordFailure(PARTITION_0, SHORT_LATENCY);
            }
            for (int i = 0; i < 2; i++) {
                monitor.recordSuccess(PARTITION_0, SHORT_LATENCY);
            }
            monitor.evaluate(PARTITION_0);
            assertThat(emittedSignals).hasSize(2);
        }
    }
}
