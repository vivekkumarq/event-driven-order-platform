package com.vivek.platform.inventory.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published by order-service on {@code order-created-topic} and consumed here to reserve stock.
 *
 * <p>{@code eventId} is the de-duplication key: consumers record it and skip redeliveries.
 */
public record OrderCreatedEvent(
        UUID eventId,
        UUID orderId,
        String sku,
        int quantity,
        BigDecimal amount,
        Instant occurredAt) {

    public static OrderCreatedEvent of(UUID orderId, String sku, int quantity, BigDecimal amount) {
        return new OrderCreatedEvent(UUID.randomUUID(), orderId, sku, quantity, amount, Instant.now());
    }
}
