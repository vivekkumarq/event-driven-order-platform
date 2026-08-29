package com.vivek.platform.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topics this service produces to: the reservation result topic it owns, and the
 * dead-letter topics for the two topics it consumes.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic inventoryReservationResultTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.inventoryReservationResult()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderCreatedDltTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.orderCreated() + KafkaTopicsProperties.DLT_SUFFIX)
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic orderCancelledDltTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.orderCancelled() + KafkaTopicsProperties.DLT_SUFFIX)
                .partitions(1).replicas(1).build();
    }
}
