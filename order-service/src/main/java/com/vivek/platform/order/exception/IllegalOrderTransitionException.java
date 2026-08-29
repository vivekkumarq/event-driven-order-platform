package com.vivek.platform.order.exception;

import com.vivek.platform.order.domain.OrderStatus;

/** Thrown when a caller asks for a status change the lifecycle forbids. Mapped to HTTP 409. */
public class IllegalOrderTransitionException extends RuntimeException {

    public IllegalOrderTransitionException(OrderStatus from, OrderStatus to) {
        super("Illegal order status transition: " + from + " -> " + to);
    }
}
