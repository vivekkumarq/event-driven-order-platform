package com.vivek.platform.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Optimistic-locking retry policy for reservations, bound from
 * {@code platform.inventory.reservation.*}.
 *
 * @param maxAttempts total attempts, including the first
 * @param backoffMs   base delay between attempts; the delay grows linearly with the attempt number
 */
@ConfigurationProperties(prefix = "platform.inventory.reservation")
public record ReservationProperties(int maxAttempts, long backoffMs) {
}
