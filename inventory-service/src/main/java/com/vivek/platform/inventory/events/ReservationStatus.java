package com.vivek.platform.inventory.events;

/** Outcome of a stock reservation attempt, as reported by inventory-service. */
public enum ReservationStatus {
    RESERVED,
    FAILED
}
