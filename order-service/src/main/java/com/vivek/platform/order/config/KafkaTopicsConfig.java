package com.vivek.platform.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topics this service owns so they are created with deliberate partition and
 * replication settings instead of relying on broker-side auto-creation.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic orderCreatedTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.orderCreated()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderCreatedDltTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.orderCreated() + KafkaTopicsProperties.DLT_SUFFIX)
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic orderCancelledTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.orderCancelled()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderCancelledDltTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.orderCancelled() + KafkaTopicsProperties.DLT_SUFFIX)
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservationResultTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.inventoryReservationResult()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservationResultDltTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.inventoryReservationResultDlt()).partitions(1).replicas(1).build();
    }
}
