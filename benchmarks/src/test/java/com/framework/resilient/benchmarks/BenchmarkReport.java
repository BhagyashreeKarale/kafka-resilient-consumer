package com.framework.resilient.benchmarks;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Machine-parseable benchmark report model containing all measured metrics,
 * 95% confidence intervals, test parameters, and partition configuration.
 * Serializable to JSON via Jackson.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BenchmarkReport {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .configure(SerializationFeature.INDENT_OUTPUT, true);

    private static final ObjectMapper COMPACT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    // --- Test metadata ---
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant startTime;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant endTime;
    private long testDurationMs;
    private long totalEventsProcessed;
    private int partitionCount;
    private String kafkaBootstrapServers;

    // --- Error injection parameters ---
    private Map<String, Object> errorInjectionParams = new HashMap<>();

    // --- Metrics ---
    private MetricResult baselineThroughput;
    private MetricResult isolatedThroughput;
    private Double throughputDegradationPercent;

    private LatencyMetricResult deduplicationLatency;
    private LatencyMetricResult reorderLatency;

    private MetricResult recoveryTimeMs;
    private MetricResult rebalanceDurationMs;
    private MetricResult dlqThroughput;
    private MetricResult memoryBytesPerEvent;

    // --- Constructors ---

    public BenchmarkReport() {
    }

    // --- Metric result with 95% confidence interval ---

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class MetricResult {
        private double mean;
        private double stdDev;
        private double confidenceIntervalLow;
        private double confidenceIntervalHigh;
        private long sampleCount;
        private String unit;

        public MetricResult() {
        }

        public MetricResult(double mean, double stdDev, long sampleCount, String unit) {
            this.mean = mean;
            this.stdDev = stdDev;
            this.sampleCount = sampleCount;
            this.unit = unit;
            // 95% confidence interval: mean ± 1.96 * stdDev / sqrt(n)
            double marginOfError = sampleCount > 1
                    ? 1.96 * stdDev / Math.sqrt(sampleCount)
                    : 0.0;
            this.confidenceIntervalLow = mean - marginOfError;
            this.confidenceIntervalHigh = mean + marginOfError;
        }

        public double getMean() { return mean; }
        public void setMean(double mean) { this.mean = mean; }
        public double getStdDev() { return stdDev; }
        public void setStdDev(double stdDev) { this.stdDev = stdDev; }
        public double getConfidenceIntervalLow() { return confidenceIntervalLow; }
        public void setConfidenceIntervalLow(double confidenceIntervalLow) { this.confidenceIntervalLow = confidenceIntervalLow; }
        public double getConfidenceIntervalHigh() { return confidenceIntervalHigh; }
        public void setConfidenceIntervalHigh(double confidenceIntervalHigh) { this.confidenceIntervalHigh = confidenceIntervalHigh; }
        public long getSampleCount() { return sampleCount; }
        public void setSampleCount(long sampleCount) { this.sampleCount = sampleCount; }
        public String getUnit() { return unit; }
        public void setUnit(String unit) { this.unit = unit; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class LatencyMetricResult {
        private double p50;
        private double p95;
        private double p99;
        private MetricResult p50Metric;
        private MetricResult p95Metric;
        private MetricResult p99Metric;
        private long sampleCount;
        private String unit;

        public LatencyMetricResult() {
        }

        public LatencyMetricResult(double p50, double p95, double p99, long sampleCount, String unit) {
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
            this.sampleCount = sampleCount;
            this.unit = unit;
        }

        public double getP50() { return p50; }
        public void setP50(double p50) { this.p50 = p50; }
        public double getP95() { return p95; }
        public void setP95(double p95) { this.p95 = p95; }
        public double getP99() { return p99; }
        public void setP99(double p99) { this.p99 = p99; }
        public MetricResult getP50Metric() { return p50Metric; }
        public void setP50Metric(MetricResult p50Metric) { this.p50Metric = p50Metric; }
        public MetricResult getP95Metric() { return p95Metric; }
        public void setP95Metric(MetricResult p95Metric) { this.p95Metric = p95Metric; }
        public MetricResult getP99Metric() { return p99Metric; }
        public void setP99Metric(MetricResult p99Metric) { this.p99Metric = p99Metric; }
        public long getSampleCount() { return sampleCount; }
        public void setSampleCount(long sampleCount) { this.sampleCount = sampleCount; }
        public String getUnit() { return unit; }
        public void setUnit(String unit) { this.unit = unit; }
    }

    // --- Serialization ---

    public String toJson() throws IOException {
        return MAPPER.writeValueAsString(this);
    }

    public void writeToFile(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        MAPPER.writeValue(path.toFile(), this);
    }

    public void writeToFile(Path path, boolean prettyPrint) throws IOException {
        Files.createDirectories(path.getParent());
        ObjectMapper mapper = prettyPrint ? MAPPER : COMPACT_MAPPER;
        mapper.writeValue(path.toFile(), this);
    }

    public static BenchmarkReport fromJson(String json) throws IOException {
        return MAPPER.readValue(json, BenchmarkReport.class);
    }

    // --- Getters and setters ---

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }
    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }
    public long getTestDurationMs() { return testDurationMs; }
    public void setTestDurationMs(long testDurationMs) { this.testDurationMs = testDurationMs; }
    public long getTotalEventsProcessed() { return totalEventsProcessed; }
    public void setTotalEventsProcessed(long totalEventsProcessed) { this.totalEventsProcessed = totalEventsProcessed; }
    public int getPartitionCount() { return partitionCount; }
    public void setPartitionCount(int partitionCount) { this.partitionCount = partitionCount; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public void setKafkaBootstrapServers(String kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }
    public Map<String, Object> getErrorInjectionParams() { return errorInjectionParams; }
    public void setErrorInjectionParams(Map<String, Object> errorInjectionParams) { this.errorInjectionParams = errorInjectionParams; }
    public MetricResult getBaselineThroughput() { return baselineThroughput; }
    public void setBaselineThroughput(MetricResult baselineThroughput) { this.baselineThroughput = baselineThroughput; }
    public MetricResult getIsolatedThroughput() { return isolatedThroughput; }
    public void setIsolatedThroughput(MetricResult isolatedThroughput) { this.isolatedThroughput = isolatedThroughput; }
    public Double getThroughputDegradationPercent() { return throughputDegradationPercent; }
    public void setThroughputDegradationPercent(Double throughputDegradationPercent) { this.throughputDegradationPercent = throughputDegradationPercent; }
    public LatencyMetricResult getDeduplicationLatency() { return deduplicationLatency; }
    public void setDeduplicationLatency(LatencyMetricResult deduplicationLatency) { this.deduplicationLatency = deduplicationLatency; }
    public LatencyMetricResult getReorderLatency() { return reorderLatency; }
    public void setReorderLatency(LatencyMetricResult reorderLatency) { this.reorderLatency = reorderLatency; }
    public MetricResult getRecoveryTimeMs() { return recoveryTimeMs; }
    public void setRecoveryTimeMs(MetricResult recoveryTimeMs) { this.recoveryTimeMs = recoveryTimeMs; }
    public MetricResult getRebalanceDurationMs() { return rebalanceDurationMs; }
    public void setRebalanceDurationMs(MetricResult rebalanceDurationMs) { this.rebalanceDurationMs = rebalanceDurationMs; }
    public MetricResult getDlqThroughput() { return dlqThroughput; }
    public void setDlqThroughput(MetricResult dlqThroughput) { this.dlqThroughput = dlqThroughput; }
    public MetricResult getMemoryBytesPerEvent() { return memoryBytesPerEvent; }
    public void setMemoryBytesPerEvent(MetricResult memoryBytesPerEvent) { this.memoryBytesPerEvent = memoryBytesPerEvent; }
}
