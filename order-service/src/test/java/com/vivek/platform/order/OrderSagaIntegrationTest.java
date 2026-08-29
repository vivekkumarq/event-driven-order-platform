package com.vivek.platform.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.platform.order.api.dto.CreateOrderRequest;
import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import com.vivek.platform.order.events.InventoryReservationResultEvent;
import com.vivek.platform.order.events.ReservationStatus;
import com.vivek.platform.order.messaging.OutboxRelay;
import com.vivek.platform.order.repository.OrderRepository;
import com.vivek.platform.order.repository.OutboxEventRepository;
import com.vivek.platform.order.repository.ProcessedEventRepository;
import com.vivek.platform.order.service.OrderService;
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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The order-service half of the saga, exercised against a real in-process Kafka broker.
 *
 * <p>Covers the two hops this service owns: an accepted order reaching {@code order-created-topic}
 * through the outbox relay, and a reservation result arriving on
 * {@code inventory-reservation-result-topic} moving the order out of PENDING. The matching
 * inventory-service half is covered by its own {@code ReservationSagaIntegrationTest}; between them
 * the loop is covered end to end.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.admin.auto-create=false"
})
@ActiveProfiles("test")
@EmbeddedKafka(kraft = true, partitions = 1, topics = {
        "order-created-topic", "order-cancelled-topic", "inventory-reservation-result-topic"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderSagaIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private OrderService orderService;
    @Autowired
    private OutboxRelay outboxRelay;
    @Autowired
    private OrderRepository orderRepository;
    @Autowired
    private OutboxEventRepository outboxEventRepository;
    @Autowired
    private ProcessedEventRepository processedEventRepository;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAll();
        outboxEventRepository.deleteAll();
        processedEventRepository.deleteAll();
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
            testConsumer = null;
        }
    }

    private Consumer<String, String> consumerFor(String topic) {
        Map<String, Object> props = new HashMap<>(
                KafkaTestUtils.consumerProps(embeddedKafka.getBrokersAsString(),
                        "test-" + UUID.randomUUID(), "true"));
        testConsumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(testConsumer, topic);
        // The broker outlives individual tests, so skip whatever earlier tests left on the topic.
        KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(1));
        return testConsumer;
    }

    /** Polls until a record with the given key shows up, ignoring traffic from other tests. */
    private ConsumerRecord<String, String> awaitRecordWithKey(Consumer<String, String> consumer,
                                                              String key) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record :
                    KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1))) {
                if (key.equals(record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("No record with key " + key + " arrived within " + TIMEOUT);
    }

    @Test
    @DisplayName("an accepted order reaches order-created-topic once the outbox relay runs")
    void publishesOrderCreatedThroughTheOutbox() throws Exception {
        Consumer<String, String> consumer = consumerFor("order-created-topic");

        OrderEntity order = orderService.createOrder(
                new CreateOrderRequest("SKU-LAPTOP-01", 3, new BigDecimal("1499.99")));

        // Nothing is on the topic yet: the event is committed to the database, not to Kafka.
        assertThat(outboxEventRepository.findByAggregateIdOrderByCreatedAtAsc(order.getId())).hasSize(1);

        assertThat(outboxRelay.relayPendingEvents()).isEqualTo(1);

        ConsumerRecord<String, String> record = awaitRecordWithKey(consumer, order.getId().toString());

        JsonNode payload = objectMapper.readTree(record.value());
        assertThat(payload.get("orderId").asText()).isEqualTo(order.getId().toString());
        assertThat(payload.get("sku").asText()).isEqualTo("SKU-LAPTOP-01");
        assertThat(payload.get("quantity").asInt()).isEqualTo(3);
        assertThat(new BigDecimal(payload.get("amount").asText())).isEqualByComparingTo("1499.99");
        assertThat(payload.get("eventId").asText()).isNotBlank();
    }

    @Test
    @DisplayName("a RESERVED result consumed from Kafka confirms the order")
    void confirmsTheOrderWhenInventoryReserves() {
        OrderEntity order = orderService.createOrder(
                new CreateOrderRequest("SKU-1", 1, new BigDecimal("10.00")));

        publishResult(order.getId(), ReservationStatus.RESERVED, null);

        await().atMost(TIMEOUT).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertThat(orderService.getOrder(order.getId()).getStatus())
                        .isEqualTo(OrderStatus.CONFIRMED));
    }

    @Test
    @DisplayName("a FAILED result rejects the order and records why")
    void rejectsTheOrderWhenInventoryCannotReserve() {
        OrderEntity order = orderService.createOrder(
                new CreateOrderRequest("SKU-1", 99, new BigDecimal("10.00")));

        publishResult(order.getId(), ReservationStatus.FAILED, "Insufficient stock for SKU SKU-1");

        await().atMost(TIMEOUT).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            OrderEntity reloaded = orderService.getOrder(order.getId());
            assertThat(reloaded.getStatus()).isEqualTo(OrderStatus.REJECTED);
            assertThat(reloaded.getStatusReason()).isEqualTo("Insufficient stock for SKU SKU-1");
        });
    }

    @Test
    @DisplayName("the same result event delivered twice is applied once")
    void ignoresARedeliveredResult() {
        OrderEntity order = orderService.createOrder(
                new CreateOrderRequest("SKU-1", 1, new BigDecimal("10.00")));

        InventoryReservationResultEvent event = new InventoryReservationResultEvent(
                UUID.randomUUID(), order.getId(), "SKU-1", 1, ReservationStatus.RESERVED, null,
                Instant.now());
        publish(event);
        await().atMost(TIMEOUT).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertThat(orderService.getOrder(order.getId()).getStatus())
                        .isEqualTo(OrderStatus.CONFIRMED));

        // Redelivering the identical event must not throw or move the order anywhere else. Without
        // the eventId check this would attempt CONFIRMED -> CONFIRMED and blow up the listener.
        publish(event);
        publish(event);

        await().during(Duration.ofSeconds(2)).atMost(TIMEOUT).untilAsserted(() ->
                assertThat(orderService.getOrder(order.getId()).getStatus())
                        .isEqualTo(OrderStatus.CONFIRMED));
        assertThat(processedEventRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("cancelling a confirmed order puts a compensating event on order-cancelled-topic")
    void cancellingAConfirmedOrderEmitsCompensation() throws Exception {
        Consumer<String, String> consumer = consumerFor("order-cancelled-topic");

        OrderEntity order = orderService.createOrder(
                new CreateOrderRequest("SKU-1", 4, new BigDecimal("25.00")));
        outboxRelay.relayPendingEvents();

        publishResult(order.getId(), ReservationStatus.RESERVED, null);
        await().atMost(TIMEOUT).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertThat(orderService.getOrder(order.getId()).getStatus())
                        .isEqualTo(OrderStatus.CONFIRMED));

        orderService.cancelOrder(order.getId(), "customer changed their mind");
        outboxRelay.relayPendingEvents();

        ConsumerRecord<String, String> record = awaitRecordWithKey(consumer, order.getId().toString());
        JsonNode payload = objectMapper.readTree(record.value());
        assertThat(payload.get("orderId").asText()).isEqualTo(order.getId().toString());
        assertThat(payload.get("sku").asText()).isEqualTo("SKU-1");
        assertThat(payload.get("quantity").asInt()).isEqualTo(4);
        assertThat(payload.get("reason").asText()).isEqualTo("customer changed their mind");
    }

    private void publishResult(UUID orderId, ReservationStatus status, String reason) {
        publish(new InventoryReservationResultEvent(UUID.randomUUID(), orderId, "SKU-1", 1, status,
                reason, Instant.now()));
    }

    private void publish(InventoryReservationResultEvent event) {
        try {
            kafkaTemplate.send("inventory-reservation-result-topic", event.orderId().toString(),
                    objectMapper.writeValueAsString(event)).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish test event", e);
        }
    }
}
