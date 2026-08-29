package com.vivek.platform.order.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

/** Request body for creating an order. Every field is validated before the service is reached. */
@Schema(name = "CreateOrderRequest", description = "New order to place")
public record CreateOrderRequest(

        @Schema(description = "Stock keeping unit to order", example = "SKU-LAPTOP-01")
        @NotBlank
        @Size(max = 64)
        String sku,

        @Schema(description = "Units to reserve", example = "2")
        @NotNull
        @Min(1)
        @Max(1000)
        Integer quantity,

        @Schema(description = "Order total, two decimal places", example = "1499.99")
        @NotNull
        @DecimalMin(value = "0.01", message = "amount must be greater than zero")
        @Digits(integer = 17, fraction = 2)
        BigDecimal amount) {
}
