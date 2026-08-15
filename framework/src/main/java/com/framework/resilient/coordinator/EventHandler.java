package com.framework.resilient.coordinator;

/**
 * The single extension point for business logic.
 * Framework has ZERO knowledge of what T represents.
 *
 * @param <T> the deserialized event payload type
 */
@FunctionalInterface
public interface EventHandler<T> {

    /**
     * Process a single deserialized event.
     *
     * @param event    the deserialized event payload
     * @param metadata processing context (partition, offset, headers)
     * @return ProcessingResult indicating success or classified failure
     * @throws TransientProcessingException  for retryable failures
     * @throws PermanentProcessingException for non-retryable failures
     */
    ProcessingResult handle(T event, EventMetadata metadata);
}
