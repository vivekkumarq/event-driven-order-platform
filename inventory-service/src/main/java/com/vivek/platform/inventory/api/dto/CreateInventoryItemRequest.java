package com.vivek.platform.inventory.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@Schema(name = "CreateInventoryItemRequest", description = "A new SKU and its opening stock level")
public record CreateInventoryItemRequest(

        @Schema(description = "Stock keeping unit", example = "SKU-LAPTOP-01")
        @NotBlank
        @Size(max = 64)
        String sku,

        @Schema(description = "Units available to sell", example = "100")
        @NotNull
        @Min(0)
        Integer availableQuantity) {
}
