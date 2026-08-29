package com.vivek.platform.order.messaging;

import com.vivek.platform.order.config.OutboxProperties;
import com.vivek.platform.order.domain.OutboxEventEntity;
import com.vivek.platform.order.domain.OutboxStatus;
import com.vivek.platform.order.repository.OutboxEventRepository;
import com.vivek.platform.order.service.OrderMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DataJpaTest
@ActiveProfiles("test")
class OutboxRelayTest {

    @Autowired
    private OutboxEventRepository outboxRepository;

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private SimpleMeterRegistry meterRegistry;

    private OutboxRelay relay(int batchSize, int maxAttempts) {
        meterRegistry = new SimpleMeterRegistry();
        return new OutboxRelay(outboxRepository, kafkaTemplate,
                new OutboxProperties(1000, batchSize, maxAttempts, true),
                new OrderMetrics(meterRegistry, outboxRepository));
    }

    @BeforeEach
    void reset() {
        outboxRepository.deleteAll();
    }

    private OutboxEventEntity pending(String topic, String key, String payload) {
        return outboxRepository.save(new OutboxEventEntity(UUID.randomUUID(), "Order",
                UUID.randomUUID(), "OrderCreated", topic, key, payload));
    }

    private void brokerAccepts() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    @DisplayName("publishes pending rows and marks them PUBLISHED")
    void publishesPendingRows() {
        brokerAccepts();
        pending("order-created-topic", "key-1", "{\"a\":1}");
        pending("order-cancelled-topic", "key-2", "{\"a\":2}");

        OutboxRelay relay = relay(50, 10);
        int published = relay.relayPendingEvents();

        assertThat(published).isEqualTo(2);
        verify(kafkaTemplate).send("order-created-topic", "key-1", "{\"a\":1}");
        verify(kafkaTemplate).send("order-cancelled-topic", "key-2", "{\"a\":2}");
        assertThat(outboxRepository.findAll())
                .allSatisfy(row -> {
                    assertThat(row.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
                    assertThat(row.getPublishedAt()).isNotNull();
                });
        assertThat(meterRegistry.get("platform.outbox.published").counter().count()).isEqualTo(2);
    }

    @Test
    void doesNothingWhenThereIsNothingPending() {
        OutboxRelay relay = relay(50, 10);

        assertThat(relay.relayPendingEvents()).isZero();
    }

    @Test
    @DisplayName("a failed publish leaves the row PENDING so the next run retries it")
    void keepsFailedRowsPendingForRetry() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));
        pending("order-created-topic", "key-1", "{}");

        OutboxRelay relay = relay(50, 10);
        assertThat(relay.relayPendingEvents()).isZero();

        OutboxEventEntity row = outboxRepository.findAll().get(0);
        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.getAttempts()).isEqualTo(1);
        assertThat(row.getLastError()).contains("broker down");
        assertThat(meterRegistry.get("platform.outbox.publish.failures").counter().count()).isEqualTo(1);
    }

    @Test
    @DisplayName("a row is parked as FAILED once it runs out of attempts")
    void parksRowsThatExhaustTheirAttempts() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));
        pending("order-created-topic", "key-1", "{}");

        OutboxRelay relay = relay(50, 1);
        relay.relayPendingEvents();

        OutboxEventEntity row = outboxRepository.findAll().get(0);
        assertThat(row.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(row.getAttempts()).isEqualTo(1);

        // A parked row is no longer picked up.
        assertThat(relay.relayPendingEvents()).isZero();
    }

    @Test
    void honoursTheConfiguredBatchSize() {
        brokerAccepts();
        pending("order-created-topic", "key-1", "{}");
        pending("order-created-topic", "key-2", "{}");
        pending("order-created-topic", "key-3", "{}");

        OutboxRelay relay = relay(2, 10);

        assertThat(relay.relayPendingEvents()).isEqualTo(2);
        assertThat(relay.relayPendingEvents()).isEqualTo(1);
    }

    @Test
    @DisplayName("oldest events go out first, so a cancellation cannot overtake its creation")
    void publishesInCreationOrder() throws InterruptedException {
        brokerAccepts();
        // Spaced out so the createdAt values are unambiguously ordered.
        pending("order-created-topic", "first", "{}");
        Thread.sleep(5);
        pending("order-created-topic", "second", "{}");
        Thread.sleep(5);
        pending("order-created-topic", "third", "{}");

        relay(1, 10).relayPendingEvents();

        verify(kafkaTemplate).send(eq("order-created-topic"), eq("first"), anyString());
        List<OutboxEventEntity> rows = outboxRepository.findAll();
        assertThat(rows).filteredOn(row -> row.getStatus() == OutboxStatus.PUBLISHED)
                .extracting(OutboxEventEntity::getMessageKey)
                .containsExactly("first");
    }
}
