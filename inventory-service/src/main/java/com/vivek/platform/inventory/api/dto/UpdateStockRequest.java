package com.vivek.platform.inventory.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

@Schema(name = "UpdateStockRequest", description = "Replacement value for the sellable stock level")
public record UpdateStockRequest(

        @Schema(description = "New available quantity; reserved units are left untouched", example = "250")
        @NotNull
        @Min(0)
        Integer availableQuantity) {
}
