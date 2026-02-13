package com.vivek.platform.order.events;

import java.util.UUID;

public class OrderCreatedEvent {

    private UUID orderId;
    private Double amount;

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }
}
