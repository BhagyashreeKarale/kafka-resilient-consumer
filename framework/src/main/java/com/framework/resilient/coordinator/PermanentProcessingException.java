package com.framework.resilient.coordinator;

/**
 * Thrown by an {@link EventHandler} to indicate a non-retryable (permanent) failure.
 * The framework will route the event directly to the DLQ without retrying.
 */
public class PermanentProcessingException extends RuntimeException {

    public PermanentProcessingException(String message) {
        super(message);
    }

    public PermanentProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
