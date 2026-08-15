package com.framework.resilient.dlq;

/**
 * Thrown when an event cannot be deserialized from the raw Kafka record.
 * Events that fail with this exception are routed directly to the DLQ
 * with a DESERIALIZATION classification without entering subsequent pipeline stages.
 */
public class DeserializationException extends RuntimeException {

    public DeserializationException(String message) {
        super(message);
    }

    public DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
