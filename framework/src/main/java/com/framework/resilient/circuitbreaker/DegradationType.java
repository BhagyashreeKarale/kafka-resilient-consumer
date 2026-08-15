package com.framework.resilient.circuitbreaker;

/**
 * The type of health degradation detected by the partition health monitor.
 */
public enum DegradationType {

    /** Error rate exceeded the configured threshold. */
    ERROR_RATE,

    /** Processing latency (p99) exceeded the configured threshold. */
    LATENCY
}
