package com.vivek.platform.order.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tuning for the transactional outbox relay, bound from {@code platform.outbox.*}.
 *
 * @param pollIntervalMs delay between relay runs
 * @param batchSize      maximum rows published per run
 * @param maxAttempts    publish attempts before a row is parked as FAILED
 * @param enabled        allows tests to keep the relay from running in the background
 */
@ConfigurationProperties(prefix = "platform.outbox")
public record OutboxProperties(
        long pollIntervalMs,
        int batchSize,
        int maxAttempts,
        boolean enabled) {
}
