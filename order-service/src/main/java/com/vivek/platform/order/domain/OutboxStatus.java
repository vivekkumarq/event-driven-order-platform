package com.vivek.platform.order.domain;

public enum OutboxStatus {
    /** Written in the business transaction, not yet published to Kafka. */
    PENDING,
    /** Successfully handed to the broker and acknowledged. */
    PUBLISHED,
    /** Publishing failed more times than the configured maximum; needs operator attention. */
    FAILED
}
