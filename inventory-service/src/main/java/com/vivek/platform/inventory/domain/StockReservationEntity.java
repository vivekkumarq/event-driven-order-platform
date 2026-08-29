package com.vivek.platform.inventory.domain;

import com.vivek.platform.inventory.events.ReservationStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * The record of what this service decided about one OrderCreated event.
 *
 * <p>It doubles as the de-duplication table: the primary key is the incoming event id, so a
 * redelivery finds the row and skips straight to re-publishing the stored outcome instead of
 * reserving a second time.
 *
 * <p>Storing {@code resultEventId} matters. Re-publishing carries the same id it had the first
 * time, so order-service recognises the repeat and applies the result exactly once. That is also
 * what stops an order getting stuck when the original result event was written but never reached
 * the broker.
 *
 * <p>The {@link Persistable} implementation is load-bearing. The id is the incoming event id rather
 * than a generated one, so Spring Data would otherwise consider every instance an existing row and
 * call {@code merge} — which quietly overwrites a concurrent duplicate instead of rejecting it, and
 * would let the same event reserve stock twice. Forcing {@code persist} turns that race into a
 * primary key violation, which the coordinator retries and the retry then de-duplicates.
 */
@Entity
@Table(name = "stock_reservations",
        uniqueConstraints = @UniqueConstraint(name = "uk_stock_reservations_order", columnNames = "orderId"))
public class StockReservationEntity implements Persistable<UUID> {

    /** The id of the OrderCreated event that caused this decision. */
    @Id
    private UUID eventId;

    @Column(nullable = false)
    private UUID orderId;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private ReservationStatus status;

    @Column(length = 512)
    private String reason;

    /** Stable id of the outgoing result event, reused on every re-publish. */
    @Column(nullable = false)
    private UUID resultEventId;

    @Column(nullable = false)
    private boolean released;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private Instant releasedAt;

    @Transient
    private boolean newRecord;

    protected StockReservationEntity() {
        // for JPA
    }

    private StockReservationEntity(UUID eventId, UUID orderId, String sku, int quantity,
                                   ReservationStatus status, String reason) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.sku = sku;
        this.quantity = quantity;
        this.status = status;
        this.reason = reason;
        this.resultEventId = UUID.randomUUID();
        this.released = false;
        this.createdAt = Instant.now();
        this.newRecord = true;
    }

    @PostPersist
    @PostLoad
    void markNotNew() {
        this.newRecord = false;
    }

    @Override
    public UUID getId() {
        return eventId;
    }

    @Override
    public boolean isNew() {
        return newRecord;
    }

    public static StockReservationEntity reserved(UUID eventId, UUID orderId, String sku, int quantity) {
        return new StockReservationEntity(eventId, orderId, sku, quantity, ReservationStatus.RESERVED, null);
    }

    public static StockReservationEntity failed(UUID eventId, UUID orderId, String sku, int quantity,
                                                String reason) {
        return new StockReservationEntity(eventId, orderId, sku, quantity, ReservationStatus.FAILED, reason);
    }

    public void markReleased() {
        this.released = true;
        this.releasedAt = Instant.now();
    }

    public boolean isReserved() {
        return status == ReservationStatus.RESERVED;
    }

    /** True when units are still held, i.e. the reservation succeeded and was not released. */
    public boolean holdsStock() {
        return isReserved() && !released;
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getSku() {
        return sku;
    }

    public int getQuantity() {
        return quantity;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public UUID getResultEventId() {
        return resultEventId;
    }

    public boolean isReleased() {
        return released;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getReleasedAt() {
        return releasedAt;
    }
}
