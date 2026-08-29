package com.vivek.platform.order.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Compensating event published by order-service on {@code order-cancelled-topic} when an order that
 * already holds a stock reservation is cancelled. inventory-service releases the reserved units.
 */
public record OrderCancelledEvent(
        UUID eventId,
        UUID orderId,
        String sku,
        int quantity,
        String reason,
        Instant occurredAt) {

    public static OrderCancelledEvent of(UUID orderId, String sku, int quantity, String reason) {
        return new OrderCancelledEvent(UUID.randomUUID(), orderId, sku, quantity, reason, Instant.now());
    }
}
