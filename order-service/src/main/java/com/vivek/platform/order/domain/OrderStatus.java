package com.vivek.platform.order.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Lifecycle of an order.
 *
 * <p>PENDING is the initial state. From there the order is either CONFIRMED (inventory reserved the
 * stock), REJECTED (inventory could not) or CANCELLED (the caller withdrew it). A CONFIRMED order
 * can still be CANCELLED, which releases the reserved stock through a compensating event.
 * REJECTED and CANCELLED are terminal.
 */
public enum OrderStatus {

    /** Order accepted by the API; stock reservation requested but not yet answered. */
    PENDING,

    /** Inventory confirmed the reservation. */
    CONFIRMED,

    /** Inventory could not reserve the requested stock. */
    REJECTED,

    /** Cancelled by the caller. Stock already reserved is released by a compensating event. */
    CANCELLED;

    private static final Map<OrderStatus, Set<OrderStatus>> ALLOWED_TRANSITIONS = Map.of(
            PENDING, EnumSet.of(CONFIRMED, REJECTED, CANCELLED),
            CONFIRMED, EnumSet.of(CANCELLED),
            REJECTED, EnumSet.noneOf(OrderStatus.class),
            CANCELLED, EnumSet.noneOf(OrderStatus.class));

    /** Returns true when moving from this status to {@code target} is a legal transition. */
    public boolean canTransitionTo(OrderStatus target) {
        return target != null && ALLOWED_TRANSITIONS.get(this).contains(target);
    }

    /** Returns true when no further transition out of this status is legal. */
    public boolean isTerminal() {
        return ALLOWED_TRANSITIONS.get(this).isEmpty();
    }
}
