package com.vivek.platform.inventory.service;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import com.vivek.platform.inventory.domain.ProcessedEventEntity;
import com.vivek.platform.inventory.domain.StockReservationEntity;
import com.vivek.platform.inventory.events.OrderCancelledEvent;
import com.vivek.platform.inventory.events.OrderCreatedEvent;
import com.vivek.platform.inventory.repository.InventoryItemRepository;
import com.vivek.platform.inventory.repository.ProcessedEventRepository;
import com.vivek.platform.inventory.repository.StockReservationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * The transactional half of stock reservation.
 *
 * <p>Each public method is one short database transaction. The optimistic locking failure raised by
 * a concurrent update surfaces when this transaction commits, i.e. as the proxied call returns, so
 * the retry loop lives in {@link ReservationCoordinator} — a caller outside the transaction
 * boundary. Retrying inside the failed transaction would be pointless, because it is already marked
 * rollback-only.
 *
 * <p>{@link Propagation#REQUIRES_NEW} makes each attempt independent, so a failed attempt does not
 * poison the transaction of the attempt that follows it.
 */
@Service
public class StockReservationService {

    private static final Logger log = LoggerFactory.getLogger(StockReservationService.class);
    private static final String CANCELLATION_LISTENER = "order-cancelled";

    private final InventoryItemRepository itemRepository;
    private final StockReservationRepository reservationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final InventoryMetrics metrics;

    public StockReservationService(InventoryItemRepository itemRepository,
                                   StockReservationRepository reservationRepository,
                                   ProcessedEventRepository processedEventRepository,
                                   InventoryMetrics metrics) {
        this.itemRepository = itemRepository;
        this.reservationRepository = reservationRepository;
        this.processedEventRepository = processedEventRepository;
        this.metrics = metrics;
    }

    /**
     * Reserves stock for an order, or records why it could not.
     *
     * <p>Idempotent on {@code event.eventId()}: a redelivery returns the decision already stored and
     * touches no stock. Two racing consumers both reserving the same SKU collide on the item's
     * version column; the loser sees an optimistic locking failure propagate out of this call.
     *
     * @return the reservation record, whether it succeeded or failed
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public StockReservationEntity reserve(OrderCreatedEvent event) {
        Optional<StockReservationEntity> existing = reservationRepository.findById(event.eventId());
        if (existing.isPresent()) {
            metrics.duplicateSkipped();
            log.info("OrderCreated event {} already handled for order {}; not reserving again",
                    event.eventId(), event.orderId());
            return existing.get();
        }

        Optional<InventoryItemEntity> found = itemRepository.findBySku(event.sku());
        if (found.isEmpty()) {
            return recordFailure(event, "Unknown SKU: " + event.sku());
        }

        InventoryItemEntity item = found.get();
        if (!item.canReserve(event.quantity())) {
            return recordFailure(event, "Insufficient stock for SKU " + event.sku()
                    + ": requested " + event.quantity() + ", available " + item.getAvailableQuantity());
        }

        item.reserve(event.quantity());
        itemRepository.save(item);

        StockReservationEntity reservation = reservationRepository.save(StockReservationEntity.reserved(
                event.eventId(), event.orderId(), event.sku(), event.quantity()));
        metrics.reservationSucceeded();
        log.info("Reserved {} of {} for order {} (available now {})",
                event.quantity(), event.sku(), event.orderId(), item.getAvailableQuantity());
        return reservation;
    }

    /**
     * Releases stock held for a cancelled order.
     *
     * <p>Idempotent twice over: the cancellation event id is recorded in the processed-events table,
     * and the reservation itself is flagged released, so neither a redelivered cancellation nor a
     * second cancellation of the same order can return the units twice.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void release(OrderCancelledEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            metrics.duplicateSkipped();
            log.info("OrderCancelled event {} already handled; not releasing again", event.eventId());
            return;
        }
        processedEventRepository.save(new ProcessedEventEntity(event.eventId(), CANCELLATION_LISTENER));

        Optional<StockReservationEntity> found = reservationRepository.findByOrderId(event.orderId());
        if (found.isEmpty()) {
            log.warn("Cancellation {} for order {} has no reservation to release",
                    event.eventId(), event.orderId());
            return;
        }

        StockReservationEntity reservation = found.get();
        if (!reservation.holdsStock()) {
            log.info("Reservation for order {} holds no stock (status={}, released={}); nothing to release",
                    event.orderId(), reservation.getStatus(), reservation.isReleased());
            return;
        }

        Optional<InventoryItemEntity> item = itemRepository.findBySku(reservation.getSku());
        if (item.isEmpty()) {
            log.error("Reservation for order {} references SKU {} that no longer exists",
                    event.orderId(), reservation.getSku());
            return;
        }

        item.get().release(reservation.getQuantity());
        itemRepository.save(item.get());
        reservation.markReleased();
        reservationRepository.save(reservation);

        metrics.reservationReleased();
        log.info("Released {} of {} held for cancelled order {} (available now {})",
                reservation.getQuantity(), reservation.getSku(), event.orderId(),
                item.get().getAvailableQuantity());
    }

    private StockReservationEntity recordFailure(OrderCreatedEvent event, String reason) {
        StockReservationEntity reservation = reservationRepository.save(StockReservationEntity.failed(
                event.eventId(), event.orderId(), event.sku(), event.quantity(), reason));
        metrics.reservationFailed();
        log.info("Refused reservation for order {}: {}", event.orderId(), reason);
        return reservation;
    }
}
