package com.vivek.platform.inventory.exception;

/** Thrown when a SKU does not exist. Mapped to HTTP 404. */
public class InventoryItemNotFoundException extends RuntimeException {

    public InventoryItemNotFoundException(String sku) {
        super("Inventory item not found for SKU: " + sku);
    }
}
