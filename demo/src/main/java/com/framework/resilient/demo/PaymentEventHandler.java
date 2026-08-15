package com.framework.resilient.demo;

import com.framework.resilient.coordinator.EventHandler;
import com.framework.resilient.coordinator.EventMetadata;
import com.framework.resilient.coordinator.PermanentProcessingException;
import com.framework.resilient.coordinator.ProcessingResult;
import com.framework.resilient.coordinator.TransientProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Sample event handler for PaymentEvents.
 * Logs each event and supports configurable failure injection for demo purposes.
 *
 * <p>Set {@code demo.failure-mode} to:
 * <ul>
 *   <li>NONE — always succeeds (default)</li>
 *   <li>TRANSIENT — throws TransientProcessingException</li>
 *   <li>PERMANENT — throws PermanentProcessingException</li>
 * </ul>
 */
@Component
public class PaymentEventHandler implements EventHandler<PaymentEvent> {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventHandler.class);

    public enum FailureMode {
        NONE,
        TRANSIENT,
        PERMANENT
    }

    private final FailureMode failureMode;

    public PaymentEventHandler(@Value("${demo.failure-mode:NONE}") FailureMode failureMode) {
        this.failureMode = failureMode;
        log.info("PaymentEventHandler initialized with failure-mode={}", failureMode);
    }

    @Override
    public ProcessingResult handle(PaymentEvent event, EventMetadata metadata) {
        log.info("Processing PaymentEvent: account={}, seq={}, amount={} {}, type={}, partition={}, offset={}",
                event.accountId(),
                event.sequenceNumber(),
                event.amount(),
                event.currency(),
                event.type(),
                metadata.partition(),
                metadata.offset());

        return switch (failureMode) {
            case TRANSIENT -> throw new TransientProcessingException(
                    "Injected transient failure for account=" + event.accountId());
            case PERMANENT -> throw new PermanentProcessingException(
                    "Injected permanent failure for account=" + event.accountId());
            case NONE -> ProcessingResult.SUCCESS;
        };
    }
}
