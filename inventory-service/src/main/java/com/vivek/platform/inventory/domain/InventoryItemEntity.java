package com.vivek.platform.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

/**
 * Stock held for one SKU.
 *
 * <p>Units live in one of two buckets: {@code availableQuantity} can still be sold,
 * {@code reservedQuantity} is committed to an order that has not shipped. Reserving moves units
 * from the first to the second; releasing moves them back. Their sum is the physical stock, and
 * every operation preserves it.
 *
 * <p>The {@link Version} column is what makes concurrent reservation safe: two consumers that read
 * the same row and both try to reserve will produce the same version in their UPDATE, one of them
 * will match zero rows, and Hibernate raises an optimistic locking failure that the caller retries
 * against freshly read state. Without it the second write would silently overwrite the first and
 * oversell the SKU.
 */
@Entity
@Table(name = "inventory_items",
        uniqueConstraints = @UniqueConstraint(name = "uk_inventory_items_sku", columnNames = "sku"))
public class InventoryItemEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(nullable = false)
    private int availableQuantity;

    @Column(nullable = false)
    private int reservedQuantity;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    @Version
    private Long version;

    protected InventoryItemEntity() {
        // for JPA
    }

    public InventoryItemEntity(String sku, int availableQuantity) {
        if (availableQuantity < 0) {
            throw new IllegalArgumentException("availableQuantity must not be negative");
        }
        this.sku = sku;
        this.availableQuantity = availableQuantity;
        this.reservedQuantity = 0;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public boolean canReserve(int quantity) {
        return quantity > 0 && availableQuantity >= quantity;
    }

    /**
     * Moves {@code quantity} units from available to reserved.
     *
     * @throws IllegalArgumentException when there is not enough available stock
     */
    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalArgumentException("Cannot reserve " + quantity + " of " + sku
                    + ": only " + availableQuantity + " available");
        }
        this.availableQuantity -= quantity;
        this.reservedQuantity += quantity;
        this.updatedAt = Instant.now();
    }

    /**
     * Moves up to {@code quantity} units back from reserved to available. Clamped so a duplicate
     * release can never push the reserved bucket negative.
     */
    public void release(int quantity) {
        int toRelease = Math.min(Math.max(quantity, 0), reservedQuantity);
        this.reservedQuantity -= toRelease;
        this.availableQuantity += toRelease;
        this.updatedAt = Instant.now();
    }

    /** Replaces the sellable stock level, e.g. after a delivery or a stock count. */
    public void setAvailableQuantity(int availableQuantity) {
        if (availableQuantity < 0) {
            throw new IllegalArgumentException("availableQuantity must not be negative");
        }
        this.availableQuantity = availableQuantity;
        this.updatedAt = Instant.now();
    }

    public int getTotalQuantity() {
        return availableQuantity + reservedQuantity;
    }

    public UUID getId() {
        return id;
    }

    public String getSku() {
        return sku;
    }

    public int getAvailableQuantity() {
        return availableQuantity;
    }

    public int getReservedQuantity() {
        return reservedQuantity;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Long getVersion() {
        return version;
    }
}
