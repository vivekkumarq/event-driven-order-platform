package com.vivek.platform.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Topic names, externalised so the same jar can run against differently named topics.
 * Bound from the {@code platform.kafka.topics.*} keys.
 */
@ConfigurationProperties(prefix = "platform.kafka.topics")
public record KafkaTopicsProperties(
        String orderCreated,
        String orderCancelled,
        String inventoryReservationResult) {

    /** Suffix appended by the dead-letter recoverer to build a topic's DLT name. */
    public static final String DLT_SUFFIX = ".DLT";

    public String inventoryReservationResultDlt() {
        return inventoryReservationResult + DLT_SUFFIX;
    }
}
