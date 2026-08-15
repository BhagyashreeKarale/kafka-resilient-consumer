package com.framework.resilient.circuitbreaker;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Time-bucketed sliding window for partition health metrics.
 * Uses a fixed-size circular buffer of 1-second buckets.
 *
 * <p>Thread-safe: uses AtomicLong for counts and synchronized access for latency lists.
 * Stale buckets (older than the window size from now) are ignored in calculations.
 */
public class SlidingWindow {

    private final int windowSizeSeconds;
    private final Bucket[] buckets;
    private final AtomicInteger currentIndex;
    private volatile Instant windowStart;
    private final Clock clock;

    /**
     * Creates a SlidingWindow with the specified window size using the system clock.
     *
     * @param windowSizeSeconds the number of 1-second buckets in the window
     */
    public SlidingWindow(int windowSizeSeconds) {
        this(windowSizeSeconds, Clock.systemUTC());
    }

    /**
     * Creates a SlidingWindow with the specified window size and clock (for testability).
     *
     * @param windowSizeSeconds the number of 1-second buckets in the window
     * @param clock             the clock to use for time-based operations
     */
    public SlidingWindow(int windowSizeSeconds, Clock clock) {
        if (windowSizeSeconds < 1) {
            throw new IllegalArgumentException("Window size must be at least 1 second");
        }
        this.windowSizeSeconds = windowSizeSeconds;
        this.buckets = new Bucket[windowSizeSeconds];
        this.currentIndex = new AtomicInteger(0);
        this.clock = clock;
        this.windowStart = clock.instant();

        for (int i = 0; i < windowSizeSeconds; i++) {
            buckets[i] = new Bucket(clock.instant());
        }
    }

    /**
     * Records a successful event with the given processing latency.
     *
     * @param latency the processing duration for the event
     */
    public void recordSuccess(Duration latency) {
        Bucket bucket = getCurrentBucket();
        bucket.successCount.incrementAndGet();
        bucket.addLatency(latency);
    }

    /**
     * Records a failed event with the given processing latency.
     *
     * @param latency the processing duration for the event
     */
    public void recordFailure(Duration latency) {
        Bucket bucket = getCurrentBucket();
        bucket.failureCount.incrementAndGet();
        bucket.addLatency(latency);
    }

    /**
     * Returns the error rate across all active (non-stale) buckets.
     * Error rate = failures / (successes + failures).
     *
     * @return error rate as a value between 0.0 and 1.0, or 0.0 if no events recorded
     */
    public double getErrorRate() {
        long totalSuccess = 0;
        long totalFailure = 0;
        Instant cutoff = getCutoff();

        for (Bucket bucket : buckets) {
            if (isActive(bucket, cutoff)) {
                totalSuccess += bucket.successCount.get();
                totalFailure += bucket.failureCount.get();
            }
        }

        long total = totalSuccess + totalFailure;
        if (total == 0) {
            return 0.0;
        }
        return (double) totalFailure / total;
    }

    /**
     * Computes latency percentiles (p50, p95, p99) across all active buckets.
     *
     * @return LatencyPercentiles record with p50, p95, p99, or Duration.ZERO values if no data
     */
    public LatencyPercentiles getLatencyPercentiles() {
        List<Long> allLatencies = new ArrayList<>();
        Instant cutoff = getCutoff();

        for (Bucket bucket : buckets) {
            if (isActive(bucket, cutoff)) {
                synchronized (bucket.latencies) {
                    allLatencies.addAll(bucket.latencies);
                }
            }
        }

        if (allLatencies.isEmpty()) {
            return new LatencyPercentiles(Duration.ZERO, Duration.ZERO, Duration.ZERO);
        }

        Collections.sort(allLatencies);
        int size = allLatencies.size();

        Duration p50 = Duration.ofNanos(allLatencies.get(percentileIndex(size, 50)));
        Duration p95 = Duration.ofNanos(allLatencies.get(percentileIndex(size, 95)));
        Duration p99 = Duration.ofNanos(allLatencies.get(percentileIndex(size, 99)));

        return new LatencyPercentiles(p50, p95, p99);
    }

    /**
     * Returns the throughput as events per second across the elapsed window time.
     * Throughput = total events / elapsed seconds (capped at windowSizeSeconds).
     *
     * @return throughput in events per second, or 0.0 if no time has elapsed
     */
    public double getThroughput() {
        long totalEvents = 0;
        Instant cutoff = getCutoff();

        for (Bucket bucket : buckets) {
            if (isActive(bucket, cutoff)) {
                totalEvents += bucket.successCount.get() + bucket.failureCount.get();
            }
        }

        if (totalEvents == 0) {
            return 0.0;
        }

        Instant now = clock.instant();
        double elapsedSeconds = Duration.between(windowStart, now).toMillis() / 1000.0;
        // Cap elapsed time at the window size
        elapsedSeconds = Math.min(elapsedSeconds, windowSizeSeconds);

        if (elapsedSeconds <= 0) {
            return totalEvents;
        }

        return totalEvents / elapsedSeconds;
    }

    /**
     * Returns the total number of events (successes + failures) across active buckets.
     *
     * @return total event count in the current window
     */
    public int getTotalEvents() {
        long total = 0;
        Instant cutoff = getCutoff();

        for (Bucket bucket : buckets) {
            if (isActive(bucket, cutoff)) {
                total += bucket.successCount.get() + bucket.failureCount.get();
            }
        }
        return (int) total;
    }

    /**
     * Returns the start time of this sliding window.
     *
     * @return the window start instant
     */
    public Instant getWindowStart() {
        return windowStart;
    }

    /**
     * Resets all bucket data, clearing the entire sliding window.
     */
    public void reset() {
        Instant now = clock.instant();
        this.windowStart = now;
        this.currentIndex.set(0);

        for (int i = 0; i < windowSizeSeconds; i++) {
            buckets[i] = new Bucket(now);
        }
    }

    /**
     * Gets the current bucket, advancing to a new bucket if the current one is stale (>1 second old).
     * Synchronized to prevent race conditions on bucket advancement when called from
     * multiple threads (poll loop recording + health monitor evaluation).
     */
    private synchronized Bucket getCurrentBucket() {
        Instant now = clock.instant();
        int idx = currentIndex.get();
        Bucket current = buckets[idx];

        long elapsedFromBucket = Duration.between(current.bucketStart, now).toMillis();

        if (elapsedFromBucket < 1000) {
            // Still within the current bucket's 1-second window
            return current;
        }

        // Need to advance to a new bucket
        int bucketsToAdvance = (int) Math.min(elapsedFromBucket / 1000, windowSizeSeconds);
        int newIdx = (idx + bucketsToAdvance) % windowSizeSeconds;

        // Clear any intermediate buckets that were skipped (they're now stale)
        for (int i = 1; i < bucketsToAdvance; i++) {
            int clearIdx = (idx + i) % windowSizeSeconds;
            buckets[clearIdx] = new Bucket(now.minus(Duration.ofSeconds(bucketsToAdvance - i)));
        }

        // Reset the new bucket
        Bucket newBucket = new Bucket(now);
        buckets[newIdx] = newBucket;
        currentIndex.set(newIdx);

        return newBucket;
    }

    /**
     * Returns the cutoff time: buckets with start time before this are considered stale.
     */
    private Instant getCutoff() {
        return clock.instant().minus(Duration.ofSeconds(windowSizeSeconds));
    }

    /**
     * Checks if a bucket is active (not stale) based on the cutoff time.
     */
    private boolean isActive(Bucket bucket, Instant cutoff) {
        return bucket != null && !bucket.bucketStart.isBefore(cutoff);
    }

    /**
     * Calculates the index for a given percentile in a sorted list.
     */
    private int percentileIndex(int size, int percentile) {
        int index = (int) Math.ceil((percentile / 100.0) * size) - 1;
        return Math.max(0, Math.min(index, size - 1));
    }

    /**
     * A single 1-second time bucket tracking success/failure counts and latency values.
     * Thread-safe for concurrent recording.
     */
    static class Bucket {
        final AtomicLong successCount;
        final AtomicLong failureCount;
        final List<Long> latencies; // latency values in nanoseconds
        volatile Instant bucketStart;

        Bucket(Instant bucketStart) {
            this.successCount = new AtomicLong(0);
            this.failureCount = new AtomicLong(0);
            this.latencies = Collections.synchronizedList(new ArrayList<>());
            this.bucketStart = bucketStart;
        }

        void addLatency(Duration latency) {
            latencies.add(latency.toNanos());
        }
    }
}
