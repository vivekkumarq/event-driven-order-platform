package com.vivek.platform.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Consumer retry policy, bound from {@code platform.kafka.retry.*}.
 *
 * @param maxAttempts number of <em>re</em>-deliveries after the first failure, before the record is
 *                    handed to the dead-letter topic
 * @param backoffMs   fixed delay between those attempts
 */
@ConfigurationProperties(prefix = "platform.kafka.retry")
public record KafkaRetryProperties(long maxAttempts, long backoffMs) {
}
