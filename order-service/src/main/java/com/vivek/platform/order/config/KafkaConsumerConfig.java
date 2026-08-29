package com.vivek.platform.order.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.platform.order.events.InventoryReservationResultEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer wiring for the reservation-result topic.
 *
 * <p>Values are deserialised with an explicitly typed {@link JsonDeserializer} and type headers
 * disabled, so the contract is the JSON shape on the wire rather than a Java class name written by
 * the producer. That keeps the two services independently deployable.
 *
 * <p>Failures are retried with a fixed backoff and then parked on a dead-letter topic. Errors that
 * can never succeed on a retry (a malformed payload, most obviously) skip the retries and go
 * straight to the DLT.
 */
@Configuration
@EnableConfigurationProperties({KafkaTopicsProperties.class, OutboxProperties.class,
        KafkaRetryProperties.class})
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, InventoryReservationResultEvent> reservationResultConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        JsonDeserializer<InventoryReservationResultEvent> valueDeserializer =
                new JsonDeserializer<>(InventoryReservationResultEvent.class, objectMapper, false);
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                new ErrorHandlingDeserializer<>(valueDeserializer));
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> deadLetterKafkaTemplate,
                                                 MeterRegistry meterRegistry,
                                                 KafkaRetryProperties retryProperties) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new MeteredDeadLetterRecoverer(deadLetterKafkaTemplate, meterRegistry),
                new FixedBackOff(retryProperties.backoffMs(), retryProperties.maxAttempts()));
        errorHandler.addNotRetryableExceptions(DeserializationException.class, ConversionException.class);
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryReservationResultEvent>
            reservationResultListenerContainerFactory(
                    ConsumerFactory<String, InventoryReservationResultEvent> consumerFactory,
                    DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, InventoryReservationResultEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
