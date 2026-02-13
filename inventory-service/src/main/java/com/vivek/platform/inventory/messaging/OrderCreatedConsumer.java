package com.vivek.platform.inventory.messaging;

import com.vivek.platform.inventory.events.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedConsumer {

    @KafkaListener(topics = "order-created-topic", groupId = "inventory-service-group")
    public void handleOrderCreated(OrderCreatedEvent event) {
        System.out.println("📦 Inventory received OrderCreated event: " + event.getOrderId()
                + ", amount=" + event.getAmount());
    }
}
