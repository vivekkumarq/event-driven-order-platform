package com.vivek.platform.order.exception;

import java.util.UUID;

/** Thrown when an order id does not resolve. Mapped to HTTP 404 by the exception handler. */
public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(UUID id) {
        super("Order not found: " + id);
    }
}
