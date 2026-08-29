package com.vivek.platform.order.events;

/** Outcome of a stock reservation attempt, as reported by inventory-service. */
public enum ReservationStatus {
    RESERVED,
    FAILED
}
