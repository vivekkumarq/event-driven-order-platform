package com.vivek.platform.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.platform.inventory.config.KafkaTopicsProperties;
import com.vivek.platform.inventory.domain.StockReservationEntity;
import com.vivek.platform.inventory.events.InventoryReservationResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Publishes reservation outcomes back to order-service.
 *
 * <p>The event id comes from the stored reservation rather than being generated here, so a
 * re-publish after a redelivery is byte-identical in identity and order-service de-duplicates it.
 */
@Component
public class InventoryEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTopicsProperties topics;
    private final ObjectMapper objectMapper;

    public InventoryEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                   KafkaTopicsProperties topics,
                                   ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.objectMapper = objectMapper;
    }

    public void publishReservationResult(StockReservationEntity reservation) {
        InventoryReservationResultEvent event = new InventoryReservationResultEvent(
                reservation.getResultEventId(),
                reservation.getOrderId(),
                reservation.getSku(),
                reservation.getQuantity(),
                reservation.getStatus(),
                reservation.getReason(),
                Instant.now());

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialise reservation result for order "
                    + reservation.getOrderId(), e);
        }

        kafkaTemplate.send(topics.inventoryReservationResult(), reservation.getOrderId().toString(), payload);
        log.info("Published reservation result {} for order {}: {}",
                event.eventId(), event.orderId(), event.status());
    }
}
