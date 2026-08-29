package com.vivek.platform.inventory.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.platform.inventory.events.OrderCancelledEvent;
import com.vivek.platform.inventory.events.OrderCreatedEvent;
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
 * Consumer wiring for the two topics this service listens to.
 *
 * <p>Values are deserialised with explicitly typed {@link JsonDeserializer}s and type headers
 * disabled: the contract between the services is the JSON shape, not a Java class name written into
 * the message headers by the producer. That is what lets the two services keep their own copies of
 * the event records and stay independently deployable.
 *
 * <p>Both containers share one {@link DefaultErrorHandler}: retry with a fixed backoff, then park
 * the record on {@code <topic>.DLT}. A payload that cannot be deserialised can never succeed on a
 * retry, so it skips the backoff and goes straight to the dead-letter topic.
 */
@Configuration
@EnableConfigurationProperties({KafkaTopicsProperties.class, KafkaRetryProperties.class,
        ReservationProperties.class})
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> orderCreatedConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        return typedConsumerFactory(kafkaProperties, objectMapper, OrderCreatedEvent.class);
    }

    @Bean
    public ConsumerFactory<String, OrderCancelledEvent> orderCancelledConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        return typedConsumerFactory(kafkaProperties, objectMapper, OrderCancelledEvent.class);
    }

    private <T> ConsumerFactory<String, T> typedConsumerFactory(KafkaProperties kafkaProperties,
                                                                ObjectMapper objectMapper,
                                                                Class<T> type) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>(type, objectMapper, false);
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
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>
            orderCreatedListenerContainerFactory(ConsumerFactory<String, OrderCreatedEvent> consumerFactory,
                                                 DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCancelledEvent>
            orderCancelledListenerContainerFactory(
                    ConsumerFactory<String, OrderCancelledEvent> consumerFactory,
                    DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, OrderCancelledEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
