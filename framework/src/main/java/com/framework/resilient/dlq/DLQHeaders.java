package com.framework.resilient.dlq;

/**
 * Standard header names added to every DLQ-routed event for traceability.
 */
public final class DLQHeaders {

    private DLQHeaders() {
        // Utility class — prevent instantiation
    }

    /** The original topic from which the event was consumed. */
    public static final String ORIGINAL_TOPIC = "x-dlq-original-topic";

    /** The original partition number from which the event was consumed. */
    public static final String ORIGINAL_PARTITION = "x-dlq-original-partition";

    /** The original offset of the event within its partition. */
    public static final String ORIGINAL_OFFSET = "x-dlq-original-offset";

    /** The error classification that triggered DLQ routing. */
    public static final String ERROR_CLASSIFICATION = "x-dlq-error-classification";

    /** ISO-8601 timestamp of when the failure occurred. */
    public static final String FAILURE_TIMESTAMP = "x-dlq-failure-timestamp";

    /** Number of retry attempts made before DLQ routing. */
    public static final String RETRY_COUNT = "x-dlq-retry-count";

    /** The source entity identifier of the failed event. */
    public static final String SOURCE_ENTITY = "x-dlq-source-entity";

    /** Fully qualified class name of the exception that caused the failure. */
    public static final String EXCEPTION_CLASS = "x-dlq-exception-class";

    /** Sanitized exception message (may be truncated). */
    public static final String EXCEPTION_MESSAGE = "x-dlq-exception-message";

    /** Correlation ID for end-to-end tracing across pipeline stages. */
    public static final String CORRELATION_ID = "x-dlq-correlation-id";
}
