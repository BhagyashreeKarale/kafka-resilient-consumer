package com.framework.resilient.coordinator;

import java.time.Duration;

/**
 * Simple lifecycle interface for components that require startup and shutdown management.
 */
public interface Lifecycle {

    /**
     * Start the component. This method should be non-blocking and return
     * once the component is ready to operate.
     */
    void start();

    /**
     * Initiate a graceful shutdown of the component.
     *
     * @param timeout maximum time to wait for graceful shutdown before forcing closure
     */
    void shutdown(Duration timeout);
}
