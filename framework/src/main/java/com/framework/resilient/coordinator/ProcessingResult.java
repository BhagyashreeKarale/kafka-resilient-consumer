package com.framework.resilient.coordinator;

/**
 * Result of handler invocation indicating the outcome of event processing.
 */
public enum ProcessingResult {

    /** Event processed successfully. */
    SUCCESS,

    /** Event processing failed with a transient (retryable) error. */
    TRANSIENT_FAILURE,

    /** Event processing failed with a permanent (non-retryable) error. */
    PERMANENT_FAILURE
}
