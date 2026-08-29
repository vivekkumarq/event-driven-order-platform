package com.vivek.platform.inventory.api.dto;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(name = "InventoryItemResponse", description = "Stock position for one SKU")
public record InventoryItemResponse(
        UUID id,
        String sku,
        @Schema(description = "Units that can still be sold") int availableQuantity,
        @Schema(description = "Units committed to orders that have not shipped") int reservedQuantity,
        @Schema(description = "available + reserved") int totalQuantity,
        @Schema(description = "Optimistic locking version") Long version,
        Instant createdAt,
        Instant updatedAt) {

    public static InventoryItemResponse from(InventoryItemEntity entity) {
        return new InventoryItemResponse(
                entity.getId(),
                entity.getSku(),
                entity.getAvailableQuantity(),
                entity.getReservedQuantity(),
                entity.getTotalQuantity(),
                entity.getVersion(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }
}
