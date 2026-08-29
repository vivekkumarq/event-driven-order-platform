package com.vivek.platform.order.api.dto;

import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * API view of an order. Keeping this separate from {@code OrderEntity} means the persistence model
 * can change without silently changing the public contract.
 */
@Schema(name = "OrderResponse", description = "An order and its current lifecycle state")
public record OrderResponse(
        UUID id,
        String sku,
        int quantity,
        BigDecimal amount,
        OrderStatus status,
        String statusReason,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse from(OrderEntity entity) {
        return new OrderResponse(
                entity.getId(),
                entity.getSku(),
                entity.getQuantity(),
                entity.getAmount(),
                entity.getStatus(),
                entity.getStatusReason(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }
}
