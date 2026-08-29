package com.vivek.platform.inventory.service;

import com.vivek.platform.inventory.config.ReservationProperties;
import com.vivek.platform.inventory.domain.StockReservationEntity;
import com.vivek.platform.inventory.events.OrderCancelledEvent;
import com.vivek.platform.inventory.events.OrderCreatedEvent;
import com.vivek.platform.inventory.messaging.InventoryEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Drives a reservation from event to outcome, outside any transaction.
 *
 * <p>Two things have to happen here rather than in {@link StockReservationService}:
 *
 * <ul>
 *   <li><strong>Retrying a lost race.</strong> The conflict is only detected when the transaction
 *       commits, so it can only be caught by a caller sitting outside it. Each retry is a fresh
 *       transaction that re-reads the row. The catch covers the whole
 *       {@link ConcurrencyFailureException} family rather than the optimistic case alone, because
 *       which one is raised for the same collision differs between database engines; a duplicate
 *       concurrent delivery can also be rejected by the reservation primary key, hence the
 *       {@link DataIntegrityViolationException}.</li>
 *   <li><strong>Publishing the result.</strong> Sending only after the transaction commits means an
 *       announced reservation is always a durable one. The cost is that the process can die between
 *       commit and publish; the stored {@code resultEventId} makes the eventual redelivery of the
 *       OrderCreated event re-publish the identical result, so nothing is lost.</li>
 * </ul>
 *
 * <p>A conflict that outlives the retries is rethrown, which hands the record to the container error
 * handler for backoff and, ultimately, the dead-letter topic.
 */
@Service
public class ReservationCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ReservationCoordinator.class);

    private final StockReservationService reservationService;
    private final InventoryEventPublisher publisher;
    private final ReservationProperties properties;
    private final InventoryMetrics metrics;

    public ReservationCoordinator(StockReservationService reservationService,
                                  InventoryEventPublisher publisher,
                                  ReservationProperties properties,
                                  InventoryMetrics metrics) {
        this.reservationService = reservationService;
        this.publisher = publisher;
        this.properties = properties;
        this.metrics = metrics;
    }

    /** Reserves stock for an order and announces the outcome. */
    public StockReservationEntity onOrderCreated(OrderCreatedEvent event) {
        StockReservationEntity reservation = withRetry(
                () -> reservationService.reserve(event), "reserve stock for order " + event.orderId());
        publisher.publishReservationResult(reservation);
        return reservation;
    }

    /** Releases stock held for a cancelled order. No result event: cancellation is one-way. */
    public void onOrderCancelled(OrderCancelledEvent event) {
        withRetry(() -> {
            reservationService.release(event);
            return null;
        }, "release stock for order " + event.orderId());
    }

    private <T> T withRetry(Attempt<T> attempt, String description) {
        int attemptNumber = 0;
        while (true) {
            attemptNumber++;
            try {
                return attempt.run();
            } catch (ConcurrencyFailureException | DataIntegrityViolationException e) {
                metrics.optimisticLockRetry();
                if (attemptNumber >= properties.maxAttempts()) {
                    log.error("Giving up trying to {} after {} attempts", description, attemptNumber, e);
                    throw e;
                }
                log.warn("Concurrent modification while trying to {} (attempt {}/{}); retrying",
                        description, attemptNumber, properties.maxAttempts());
                sleep(properties.backoffMs() * attemptNumber);
            }
        }
    }

    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while backing off before a reservation retry", e);
        }
    }

    @FunctionalInterface
    private interface Attempt<T> {
        T run();
    }
}
