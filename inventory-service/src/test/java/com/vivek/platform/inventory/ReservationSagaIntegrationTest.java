package com.vivek.platform.inventory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.platform.inventory.domain.InventoryItemEntity;
import com.vivek.platform.inventory.events.OrderCancelledEvent;
import com.vivek.platform.inventory.events.OrderCreatedEvent;
import com.vivek.platform.inventory.repository.InventoryItemRepository;
import com.vivek.platform.inventory.repository.ProcessedEventRepository;
import com.vivek.platform.inventory.repository.StockReservationRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The inventory-service half of the saga, against a real in-process Kafka broker.
 *
 * <p>Covers the hops this service owns: an OrderCreated event consumed from
 * {@code order-created-topic} reserving stock and producing a result on
 * {@code inventory-reservation-result-topic}, the insufficient-stock branch of that, redelivery
 * being harmless, and an OrderCancelled event releasing units again. Together with order-service's
 * {@code OrderSagaIntegrationTest} the whole loop is covered.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.admin.auto-create=false"
})
@ActiveProfiles("test")
@EmbeddedKafka(kraft = true, partitions = 1, topics = {
        "order-created-topic", "order-cancelled-topic", "inventory-reservation-result-topic"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ReservationSagaIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private InventoryItemRepository itemRepository;
    @Autowired
    private StockReservationRepository reservationRepository;
    @Autowired
    private ProcessedEventRepository processedEventRepository;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> resultConsumer;

    @BeforeEach
    void setUp() {
        reservationRepository.deleteAll();
        processedEventRepository.deleteAll();
        itemRepository.deleteAll();

        Map<String, Object> props = new HashMap<>(
                KafkaTestUtils.consumerProps(embeddedKafka.getBrokersAsString(),
                        "test-" + UUID.randomUUID(), "true"));
        resultConsumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(resultConsumer, "inventory-reservation-result-topic");
        // The broker outlives individual tests, so skip whatever earlier tests left on the topic.
        KafkaTestUtils.getRecords(resultConsumer, Duration.ofSeconds(1));
    }

    @AfterEach
    void tearDown() {
        if (resultConsumer != null) {
            resultConsumer.close();
            resultConsumer = null;
        }
    }

    private JsonNode awaitResultFor(UUID orderId) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record :
                    KafkaTestUtils.getRecords(resultConsumer, Duration.ofSeconds(1))) {
                if (orderId.toString().equals(record.key())) {
                    return objectMapper.readTree(record.value());
                }
            }
        }
        throw new AssertionError("No reservation result for order " + orderId + " within " + TIMEOUT);
    }

    private void publish(String topic, UUID orderId, Object event) {
        try {
            kafkaTemplate.send(topic, orderId.toString(), objectMapper.writeValueAsString(event))
                    .get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish test event", e);
        }
    }

    private InventoryItemEntity reload(String sku) {
        return itemRepository.findBySku(sku).orElseThrow();
    }

    @Test
    @DisplayName("an OrderCreated event reserves stock and answers with RESERVED")
    void reservesStockAndPublishesTheResult() throws Exception {
        itemRepository.save(new InventoryItemEntity("SKU-LAPTOP-01", 10));
        OrderCreatedEvent event = OrderCreatedEvent.of(
                UUID.randomUUID(), "SKU-LAPTOP-01", 3, new BigDecimal("1499.99"));

        publish("order-created-topic", event.orderId(), event);

        JsonNode result = awaitResultFor(event.orderId());
        assertThat(result.get("status").asText()).isEqualTo("RESERVED");
        assertThat(result.get("sku").asText()).isEqualTo("SKU-LAPTOP-01");
        assertThat(result.get("quantity").asInt()).isEqualTo(3);
        assertThat(result.get("reason").isNull()).isTrue();
        assertThat(result.get("eventId").asText()).isNotBlank();

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(reload("SKU-LAPTOP-01").getAvailableQuantity()).isEqualTo(7);
            assertThat(reload("SKU-LAPTOP-01").getReservedQuantity()).isEqualTo(3);
        });
    }

    @Test
    @DisplayName("insufficient stock answers with FAILED and a reason, and moves no units")
    void publishesAFailureWhenStockIsInsufficient() throws Exception {
        itemRepository.save(new InventoryItemEntity("SKU-SCARCE", 2));
        OrderCreatedEvent event = OrderCreatedEvent.of(
                UUID.randomUUID(), "SKU-SCARCE", 5, new BigDecimal("99.00"));

        publish("order-created-topic", event.orderId(), event);

        JsonNode result = awaitResultFor(event.orderId());
        assertThat(result.get("status").asText()).isEqualTo("FAILED");
        assertThat(result.get("reason").asText()).contains("Insufficient stock");

        assertThat(reload("SKU-SCARCE").getAvailableQuantity()).isEqualTo(2);
        assertThat(reload("SKU-SCARCE").getReservedQuantity()).isZero();
    }

    @Test
    @DisplayName("an unknown SKU answers with FAILED rather than dead-lettering the record")
    void publishesAFailureForAnUnknownSku() throws Exception {
        OrderCreatedEvent event = OrderCreatedEvent.of(
                UUID.randomUUID(), "SKU-DOES-NOT-EXIST", 1, new BigDecimal("5.00"));

        publish("order-created-topic", event.orderId(), event);

        JsonNode result = awaitResultFor(event.orderId());
        assertThat(result.get("status").asText()).isEqualTo("FAILED");
        assertThat(result.get("reason").asText()).isEqualTo("Unknown SKU: SKU-DOES-NOT-EXIST");
    }

    @Test
    @DisplayName("the same OrderCreated event delivered three times reserves once")
    void redeliveryReservesOnce() throws Exception {
        itemRepository.save(new InventoryItemEntity("SKU-1", 20));
        OrderCreatedEvent event = OrderCreatedEvent.of(
                UUID.randomUUID(), "SKU-1", 4, new BigDecimal("10.00"));

        publish("order-created-topic", event.orderId(), event);
        assertThat(awaitResultFor(event.orderId()).get("status").asText()).isEqualTo("RESERVED");

        publish("order-created-topic", event.orderId(), event);
        publish("order-created-topic", event.orderId(), event);

        // Each redelivery re-publishes the stored decision, which carries the original event id, so
        // order-service sees them as duplicates and applies the result exactly once.
        JsonNode replay = awaitResultFor(event.orderId());
        assertThat(replay.get("status").asText()).isEqualTo("RESERVED");

        await().during(Duration.ofSeconds(2)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(reload("SKU-1").getReservedQuantity()).isEqualTo(4);
            assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(16);
        });
        assertThat(reservationRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("an OrderCancelled event releases the units the order was holding")
    void cancellationReleasesReservedStock() throws Exception {
        itemRepository.save(new InventoryItemEntity("SKU-1", 10));
        OrderCreatedEvent created = OrderCreatedEvent.of(
                UUID.randomUUID(), "SKU-1", 6, new BigDecimal("60.00"));

        publish("order-created-topic", created.orderId(), created);
        assertThat(awaitResultFor(created.orderId()).get("status").asText()).isEqualTo("RESERVED");
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(reload("SKU-1").getReservedQuantity()).isEqualTo(6));

        OrderCancelledEvent cancelled = OrderCancelledEvent.of(
                created.orderId(), "SKU-1", 6, "customer changed their mind");
        publish("order-cancelled-topic", cancelled.orderId(), cancelled);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(10);
            assertThat(reload("SKU-1").getReservedQuantity()).isZero();
        });
        assertThat(reservationRepository.findByOrderId(created.orderId()).orElseThrow().isReleased())
                .isTrue();
    }
}
