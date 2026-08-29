package com.vivek.platform.order.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Published by inventory-service on {@code inventory-reservation-result-topic} and consumed here to
 * advance the order out of PENDING. A single topic carries both outcomes, discriminated by
 * {@link ReservationStatus}: RESERVED (the InventoryReserved case) and FAILED (the
 * InventoryReservationFailed case, with {@code reason} filled in).
 */
public record InventoryReservationResultEvent(
        UUID eventId,
        UUID orderId,
        String sku,
        int quantity,
        ReservationStatus status,
        String reason,
        Instant occurredAt) {

    public boolean isReserved() {
        return status == ReservationStatus.RESERVED;
    }
}
