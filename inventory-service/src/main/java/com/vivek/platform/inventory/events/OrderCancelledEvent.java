package com.vivek.platform.inventory.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Compensating event published by order-service on {@code order-cancelled-topic} and consumed here
 * to release units held for an order that was cancelled after its reservation succeeded.
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
