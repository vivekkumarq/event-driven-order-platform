package com.vivek.platform.inventory.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** Custom application metrics, exported on {@code /actuator/prometheus}. */
@Component
public class InventoryMetrics {

    private final Counter reservationsSucceeded;
    private final Counter reservationsFailed;
    private final Counter reservationsReleased;
    private final Counter optimisticLockRetries;
    private final Counter duplicateEventsSkipped;

    public InventoryMetrics(MeterRegistry registry) {
        this.reservationsSucceeded = Counter.builder("platform.inventory.reservations.succeeded")
                .description("Stock reservations that held the requested units").register(registry);
        this.reservationsFailed = Counter.builder("platform.inventory.reservations.failed")
                .description("Stock reservations refused, e.g. insufficient stock or unknown SKU")
                .register(registry);
        this.reservationsReleased = Counter.builder("platform.inventory.reservations.released")
                .description("Reservations released by a compensating cancellation").register(registry);
        this.optimisticLockRetries = Counter.builder("platform.inventory.optimistic.lock.retries")
                .description("Reservation attempts retried after an optimistic locking conflict")
                .register(registry);
        this.duplicateEventsSkipped = Counter.builder("platform.events.duplicates.skipped")
                .description("Redelivered events skipped by the de-duplication check").register(registry);
    }

    public void reservationSucceeded() {
        reservationsSucceeded.increment();
    }

    public void reservationFailed() {
        reservationsFailed.increment();
    }

    public void reservationReleased() {
        reservationsReleased.increment();
    }

    public void optimisticLockRetry() {
        optimisticLockRetries.increment();
    }

    public void duplicateSkipped() {
        duplicateEventsSkipped.increment();
    }
}
