package com.framework.resilient.demo;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A sample payment event used to exercise the resilient consumer framework.
 *
 * @param accountId      the account identifier (used as sourceEntity for ordering)
 * @param sequenceNumber monotonically increasing sequence per account
 * @param amount         the payment amount
 * @param currency       ISO currency code (e.g., USD, EUR)
 * @param type           CREDIT or DEBIT
 * @param timestamp      when the payment event occurred
 */
public record PaymentEvent(
        String accountId,
        long sequenceNumber,
        BigDecimal amount,
        String currency,
        String type,
        Instant timestamp
) {
}
