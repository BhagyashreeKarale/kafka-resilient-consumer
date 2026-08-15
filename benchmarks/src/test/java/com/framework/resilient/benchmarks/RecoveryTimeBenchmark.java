package com.framework.resilient.benchmarks;

import com.framework.resilient.circuitbreaker.CircuitBreakerProperties;
import com.framework.resilient.circuitbreaker.CircuitState;
import com.framework.resilient.circuitbreaker.DegradationSignal;
import com.framework.resilient.circuitbreaker.DegradationType;
import com.framework.resilient.circuitbreaker.PartitionCircuitBreaker;
import com.framework.resilient.circuitbreaker.ProbeResult;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Measures recovery time: elapsed time from underlying failure resolution
 * to circuit breaker returning to CLOSED state.
 *
 * <p>This benchmark operates purely in-memory — no Kafka container needed.
 * It exercises the {@link PartitionCircuitBreaker} state machine directly.
 *
 * Validates: Requirements 5.8, 5.12, 5.13
 */
class RecoveryTimeBenchmark {

    @Test
    void measureRecoveryTime() throws Exception {
        List<Double> recoveryTimeSamples = new ArrayList<>();
        int trials = 20;

        for (int trial = 0; trial < trials; trial++) {
            // Create circuit breaker with short cooldown for faster benchmarking
            Duration cooldown = Duration.ofSeconds(2);
            CircuitBreakerProperties cbProps = new CircuitBreakerProperties(
                    0.5, Duration.ofMillis(5000), cooldown, 10,
                    Duration.ofSeconds(5), Duration.ofSeconds(2), Duration.ofSeconds(1)
            );

            AtomicBoolean paused = new AtomicBoolean(false);
            AtomicBoolean resumed = new AtomicBoolean(false);

            PartitionCircuitBreaker circuitBreaker = new PartitionCircuitBreaker(
                    cbProps,
                    tp -> paused.set(true),
                    tp -> resumed.set(true)
            );

            TopicPartition partition = new TopicPartition("benchmark-events", 0);
            circuitBreaker.initializePartition(partition);

            // Trip the circuit breaker
            DegradationSignal signal = new DegradationSignal(
                    partition, DegradationType.ERROR_RATE, 0.8, 0.5, Instant.now()
            );
            circuitBreaker.onDegradationSignal(partition, signal);
            assertThat(circuitBreaker.getState(partition)).isEqualTo(CircuitState.OPEN);

            // Wait for cooldown to expire (simulates resolution of underlying issue)
            Instant resolutionTime = Instant.now();

            // Poll evaluateCooldowns until HALF_OPEN
            await().atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> {
                        circuitBreaker.evaluateCooldowns();
                        return circuitBreaker.getState(partition) == CircuitState.HALF_OPEN;
                    });

            // Simulate successful probe (underlying issue resolved)
            ProbeResult probeResult = new ProbeResult(10, 10, 0, false);
            circuitBreaker.onProbeResult(partition, probeResult);

            Instant recoveryTime = Instant.now();
            assertThat(circuitBreaker.getState(partition)).isEqualTo(CircuitState.CLOSED);

            // Measure elapsed time from resolution to CLOSED
            double elapsedMs = Duration.between(resolutionTime, recoveryTime).toMillis();
            recoveryTimeSamples.add(elapsedMs);
        }

        BenchmarkReport report = new BenchmarkReport();
        report.setPartitionCount(6);
        report.setStartTime(Instant.now());
        report.setRecoveryTimeMs(BenchmarkHarness.computeMetric(recoveryTimeSamples, "ms"));
        report.getErrorInjectionParams().put("cooldownPeriodMs", 2000);
        report.getErrorInjectionParams().put("probeBatchSize", 10);
        report.getErrorInjectionParams().put("trials", trials);
        report.setTotalEventsProcessed(trials * 10L);
        report.setEndTime(Instant.now());
        report.setTestDurationMs(Duration.between(report.getStartTime(), report.getEndTime()).toMillis());

        assertThat(recoveryTimeSamples).hasSize(trials);
        assertThat(report.getRecoveryTimeMs().getMean()).isGreaterThan(0);

        report.writeToFile(Path.of("build", "benchmark-reports", "recovery-time.json"));
    }
}
