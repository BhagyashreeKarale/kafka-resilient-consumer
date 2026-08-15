package com.framework.resilient.circuitbreaker;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Sliding window metrics tracking per partition with degradation signal emission.
 *
 * <p>Tracks error rate, latency percentiles (p50, p95, p99), and throughput per partition
 * using a configurable sliding time window. Emits degradation signals to the circuit breaker
 * when thresholds are exceeded, with suppression logic to prevent duplicate signals.
 *
 * <p>Signal suppression: once a signal fires for a given partition+metric, further signals
 * are suppressed until the metric returns below threshold and then exceeds it again.
 */
public class PartitionHealthMonitor {

    private static final Logger log = LoggerFactory.getLogger(PartitionHealthMonitor.class);

    private final ConcurrentHashMap<TopicPartition, SlidingWindow> windows;
    private final HealthMonitorProperties healthProperties;
    private final CircuitBreakerProperties circuitBreakerProperties;
    private final Consumer<DegradationSignal> signalConsumer;
    private final Set<PartitionMetricKey> suppressedSignals;

    /**
     * Constructs a PartitionHealthMonitor with the given configuration and signal callback.
     *
     * @param healthProperties        health monitor configuration (window size, evaluation interval)
     * @param circuitBreakerProperties circuit breaker configuration (error rate and latency thresholds)
     * @param signalConsumer          callback invoked when a degradation signal is emitted
     */
    public PartitionHealthMonitor(HealthMonitorProperties healthProperties,
                                  CircuitBreakerProperties circuitBreakerProperties,
                                  Consumer<DegradationSignal> signalConsumer) {
        this.healthProperties = healthProperties;
        this.circuitBreakerProperties = circuitBreakerProperties;
        this.signalConsumer = signalConsumer;
        this.windows = new ConcurrentHashMap<>();
        this.suppressedSignals = ConcurrentHashMap.newKeySet();
    }

    // --- Metric recording ---

    /**
     * Records a successful event processing for the given partition.
     *
     * @param partition the partition where the event was processed
     * @param latency   the processing duration for the event
     */
    public void recordSuccess(TopicPartition partition, Duration latency) {
        SlidingWindow window = windows.get(partition);
        if (window == null) {
            log.warn("recordSuccess called for untracked partition: {}", partition);
            return;
        }
        window.recordSuccess(latency);
    }

    /**
     * Records a failed event processing for the given partition.
     *
     * @param partition the partition where the event failed
     * @param latency   the processing duration for the event
     */
    public void recordFailure(TopicPartition partition, Duration latency) {
        SlidingWindow window = windows.get(partition);
        if (window == null) {
            log.warn("recordFailure called for untracked partition: {}", partition);
            return;
        }
        window.recordFailure(latency);
    }

    // --- Health evaluation and signal emission ---

    /**
     * Evaluates the health of the specified partition against configured thresholds.
     * If the error rate or p99 latency exceeds the threshold, emits a degradation signal
     * (subject to suppression rules).
     *
     * <p>Suppression logic:
     * <ul>
     *   <li>If a metric is below threshold AND currently suppressed, remove suppression (un-suppress)</li>
     *   <li>If a metric exceeds threshold AND is not suppressed, emit signal and add suppression</li>
     *   <li>If a metric exceeds threshold AND is suppressed, skip (signal already fired)</li>
     * </ul>
     *
     * @param partition the partition to evaluate
     */
    public void evaluate(TopicPartition partition) {
        SlidingWindow window = windows.get(partition);
        if (window == null) {
            log.debug("evaluate called for untracked partition: {}", partition);
            return;
        }

        evaluateErrorRate(partition, window);
        evaluateLatency(partition, window);
    }

    private void evaluateErrorRate(TopicPartition partition, SlidingWindow window) {
        double errorRate = window.getErrorRate();
        double threshold = circuitBreakerProperties.errorRateThreshold();
        PartitionMetricKey key = new PartitionMetricKey(partition, DegradationType.ERROR_RATE);

        if (errorRate < threshold) {
            // Metric is below threshold — un-suppress if previously suppressed
            if (suppressedSignals.contains(key)) {
                suppressedSignals.remove(key);
                log.debug("Error rate for partition {} dropped below threshold ({} < {}), un-suppressed signal",
                        partition, errorRate, threshold);
            }
        } else {
            // Metric exceeds threshold
            if (!suppressedSignals.contains(key)) {
                // Not suppressed — emit signal and suppress
                DegradationSignal signal = new DegradationSignal(
                        partition,
                        DegradationType.ERROR_RATE,
                        errorRate,
                        threshold,
                        Instant.now()
                );
                suppressedSignals.add(key);
                log.info("Emitting degradation signal for partition {} — error rate {} exceeds threshold {}",
                        partition, errorRate, threshold);
                signalConsumer.accept(signal);
            } else {
                log.debug("Degradation signal for partition {} error rate suppressed (already fired)", partition);
            }
        }
    }

    private void evaluateLatency(TopicPartition partition, SlidingWindow window) {
        LatencyPercentiles percentiles = window.getLatencyPercentiles();
        Duration p99 = percentiles.p99();
        Duration latencyThreshold = circuitBreakerProperties.latencyThreshold();
        PartitionMetricKey key = new PartitionMetricKey(partition, DegradationType.LATENCY);

        if (p99.compareTo(latencyThreshold) < 0) {
            // Metric is below threshold — un-suppress if previously suppressed
            if (suppressedSignals.contains(key)) {
                suppressedSignals.remove(key);
                log.debug("p99 latency for partition {} dropped below threshold ({} < {}), un-suppressed signal",
                        partition, p99, latencyThreshold);
            }
        } else {
            // Metric exceeds threshold
            if (!suppressedSignals.contains(key)) {
                // Not suppressed — emit signal and suppress
                double currentValueMs = p99.toMillis();
                double thresholdMs = latencyThreshold.toMillis();
                DegradationSignal signal = new DegradationSignal(
                        partition,
                        DegradationType.LATENCY,
                        currentValueMs,
                        thresholdMs,
                        Instant.now()
                );
                suppressedSignals.add(key);
                log.info("Emitting degradation signal for partition {} — p99 latency {} exceeds threshold {}",
                        partition, p99, latencyThreshold);
                signalConsumer.accept(signal);
            } else {
                log.debug("Degradation signal for partition {} latency suppressed (already fired)", partition);
            }
        }
    }

    // --- Signal suppression management ---

    /**
     * Resets signal suppression for the specified partition and degradation type.
     * Called by the circuit breaker on recovery to allow future signals to fire again.
     *
     * @param partition the partition to reset suppression for
     * @param type      the degradation type to un-suppress
     */
    public void resetSuppression(TopicPartition partition, DegradationType type) {
        PartitionMetricKey key = new PartitionMetricKey(partition, type);
        boolean removed = suppressedSignals.remove(key);
        if (removed) {
            log.debug("Reset signal suppression for partition {} type {}", partition, type);
        }
    }

    // --- Lifecycle ---

    /**
     * Initializes an empty sliding window for a newly assigned partition.
     *
     * @param partition the partition to initialize
     */
    public void initializePartition(TopicPartition partition) {
        int windowSizeSeconds = (int) healthProperties.slidingWindowSize().toSeconds();
        windows.put(partition, new SlidingWindow(windowSizeSeconds));
        log.debug("Initialized health monitor for partition {} with {}s window", partition, windowSizeSeconds);
    }

    /**
     * Resets a partition by discarding all accumulated window data.
     * Also removes any signal suppression state for the partition.
     *
     * @param partition the partition to reset
     */
    public void resetPartition(TopicPartition partition) {
        SlidingWindow window = windows.get(partition);
        if (window != null) {
            window.reset();
        }
        // Remove suppression for both metric types
        suppressedSignals.remove(new PartitionMetricKey(partition, DegradationType.ERROR_RATE));
        suppressedSignals.remove(new PartitionMetricKey(partition, DegradationType.LATENCY));
        log.debug("Reset health monitor data for partition {}", partition);
    }

    // --- Health queries ---

    /**
     * Returns a snapshot of the partition's current health metrics.
     *
     * @param partition the partition to query
     * @return PartitionHealth record with current metrics, or null if partition is not tracked
     */
    public PartitionHealth getHealth(TopicPartition partition) {
        SlidingWindow window = windows.get(partition);
        if (window == null) {
            return null;
        }
        return new PartitionHealth(
                window.getErrorRate(),
                window.getThroughput(),
                window.getLatencyPercentiles(),
                window.getWindowStart(),
                window.getTotalEvents()
        );
    }

    /**
     * Returns the current error rate for the given partition.
     *
     * @param partition the partition to query
     * @return error rate (0.0 to 1.0), or 0.0 if partition is not tracked
     */
    public double getErrorRate(TopicPartition partition) {
        SlidingWindow window = windows.get(partition);
        if (window == null) {
            return 0.0;
        }
        return window.getErrorRate();
    }

    /**
     * Returns the latency percentiles (p50, p95, p99) for the given partition.
     *
     * @param partition the partition to query
     * @return LatencyPercentiles record, or zero-duration percentiles if partition is not tracked
     */
    public LatencyPercentiles getLatencyPercentiles(TopicPartition partition) {
        SlidingWindow window = windows.get(partition);
        if (window == null) {
            return new LatencyPercentiles(Duration.ZERO, Duration.ZERO, Duration.ZERO);
        }
        return window.getLatencyPercentiles();
    }

    /**
     * Returns the current throughput (events per second) for the given partition.
     *
     * @param partition the partition to query
     * @return throughput in events/second, or 0.0 if partition is not tracked
     */
    public double getThroughput(TopicPartition partition) {
        SlidingWindow window = windows.get(partition);
        if (window == null) {
            return 0.0;
        }
        return window.getThroughput();
    }

    // --- Internal ---

    /**
     * Private record used as a composite key for signal suppression tracking.
     * A unique combination of partition and degradation type.
     */
    private record PartitionMetricKey(TopicPartition partition, DegradationType type) {
    }
}
