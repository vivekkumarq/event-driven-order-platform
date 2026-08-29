package com.vivek.platform.inventory.messaging;

import com.vivek.platform.inventory.events.OrderCancelledEvent;
import com.vivek.platform.inventory.service.ReservationCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Compensating handler: returns units held for an order that was cancelled after reservation. */
@Component
public class OrderCancelledConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelledConsumer.class);

    private final ReservationCoordinator coordinator;

    public OrderCancelledConsumer(ReservationCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @KafkaListener(
            topics = "${platform.kafka.topics.order-cancelled}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "orderCancelledListenerContainerFactory")
    public void onOrderCancelled(OrderCancelledEvent event) {
        log.info("Received OrderCancelled {} for order {}", event.eventId(), event.orderId());
        coordinator.onOrderCancelled(event);
    }
}
