package com.vivek.platform.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders", indexes = @Index(name = "idx_orders_status", columnList = "status"))
public class OrderEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    /**
     * Monetary amount. Stored as BigDecimal with an explicit scale, never as a double, which cannot
     * represent decimal currency values exactly.
     */
    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private OrderStatus status;

    /** Populated when the order ends up REJECTED or CANCELLED. */
    @Column(length = 512)
    private String statusReason;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    @Version
    private Long version;

    protected OrderEntity() {
        // for JPA
    }

    public OrderEntity(String sku, int quantity, BigDecimal amount) {
        this.sku = sku;
        this.quantity = quantity;
        this.amount = amount;
        this.status = OrderStatus.PENDING;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }

    /**
     * Applies a status change, enforcing the {@link OrderStatus} transition rules.
     *
     * @throws IllegalStateException when the transition is not legal
     */
    public void transitionTo(OrderStatus target, String reason) {
        if (!status.canTransitionTo(target)) {
            throw new IllegalStateException(
                    "Illegal order status transition: " + status + " -> " + target);
        }
        this.status = target;
        this.statusReason = reason;
        this.updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public String getSku() {
        return sku;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public String getStatusReason() {
        return statusReason;
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
