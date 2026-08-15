package com.framework.resilient.dlq;

/**
 * Thrown when an event fails domain validation checks.
 * Events that fail with this exception are routed to the DLQ
 * with a VALIDATION classification.
 */
public class ValidationException extends RuntimeException {

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
