package com.framework.resilient.dedup;

/**
 * Result of a deduplication check against the idempotency key store.
 */
public enum DeduplicationResult {

    /** Event has not been seen before — proceed with processing. */
    NEW_EVENT,

    /** Event has already been processed — skip (discard duplicate). */
    DUPLICATE
}
