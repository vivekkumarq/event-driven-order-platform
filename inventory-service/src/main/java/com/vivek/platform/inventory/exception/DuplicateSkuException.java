package com.vivek.platform.inventory.exception;

/** Thrown when creating a SKU that already exists. Mapped to HTTP 409. */
public class DuplicateSkuException extends RuntimeException {

    public DuplicateSkuException(String sku) {
        super("Inventory item already exists for SKU: " + sku);
    }
}
