package com.vivek.platform.order.messaging;

import com.vivek.platform.order.events.InventoryReservationResultEvent;
import com.vivek.platform.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Closes the saga loop: consumes the outcome inventory-service published and advances the order.
 *
 * <p>Anything thrown here is retried with backoff by the container error handler and, once the
 * retries are exhausted, parked on the dead-letter topic.
 */
@Component
public class InventoryReservationResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservationResultConsumer.class);

    private final OrderService orderService;

    public InventoryReservationResultConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "${platform.kafka.topics.inventory-reservation-result}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "reservationResultListenerContainerFactory")
    public void onReservationResult(InventoryReservationResultEvent event) {
        log.info("Received reservation result {} for order {}: {}",
                event.eventId(), event.orderId(), event.status());
        orderService.applyReservationResult(event);
    }
}
