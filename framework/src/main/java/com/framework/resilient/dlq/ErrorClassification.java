package com.framework.resilient.dlq;

/**
 * Classification of errors that determines DLQ topic routing.
 */
public enum ErrorClassification {

    /** Event could not be deserialized from the raw Kafka record. */
    DESERIALIZATION,

    /** Event failed domain validation checks. */
    VALIDATION,

    /** Event processing failed with a transient (retryable) error after exhausting retries. */
    TRANSIENT,

    /** Event processing failed with a permanent (non-retryable) error. */
    PERMANENT
}
