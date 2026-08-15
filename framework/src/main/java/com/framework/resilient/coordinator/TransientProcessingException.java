package com.framework.resilient.coordinator;

/**
 * Thrown by an {@link EventHandler} to indicate a retryable (transient) failure.
 * The framework will retry processing according to configured retry policy
 * before routing the event to the DLQ.
 */
public class TransientProcessingException extends RuntimeException {

    public TransientProcessingException(String message) {
        super(message);
    }

    public TransientProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
