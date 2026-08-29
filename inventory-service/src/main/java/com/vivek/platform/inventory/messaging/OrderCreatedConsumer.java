package com.vivek.platform.inventory.messaging;

import com.vivek.platform.inventory.events.OrderCreatedEvent;
import com.vivek.platform.inventory.service.ReservationCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reserves stock in response to an order being placed.
 *
 * <p>Anything thrown here is retried with backoff by the container error handler and parked on the
 * dead-letter topic once the retries run out.
 */
@Component
public class OrderCreatedConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedConsumer.class);

    private final ReservationCoordinator coordinator;

    public OrderCreatedConsumer(ReservationCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @KafkaListener(
            topics = "${platform.kafka.topics.order-created}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "orderCreatedListenerContainerFactory")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreated {} for order {} sku={} quantity={}",
                event.eventId(), event.orderId(), event.sku(), event.quantity());
        coordinator.onOrderCreated(event);
    }
}
