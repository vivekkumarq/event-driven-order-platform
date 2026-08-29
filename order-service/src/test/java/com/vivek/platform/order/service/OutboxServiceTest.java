package com.vivek.platform.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vivek.platform.order.domain.OutboxEventEntity;
import com.vivek.platform.order.domain.OutboxStatus;
import com.vivek.platform.order.events.OrderCreatedEvent;
import com.vivek.platform.order.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@ActiveProfiles("test")
@Import({OutboxService.class, OutboxServiceTest.JacksonTestConfig.class})
class OutboxServiceTest {

    @TestConfiguration
    static class JacksonTestConfig {
        @Bean
        ObjectMapper objectMapper() {
            return JsonMapper.builder().addModule(new JavaTimeModule()).build();
        }
    }

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private OutboxEventRepository outboxRepository;

    @Test
    void storesTheEventAsPendingJson() {
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = OrderCreatedEvent.of(orderId, "SKU-1", 2, new BigDecimal("49.50"));

        outboxService.append(event.eventId(), "Order", orderId, "OrderCreated",
                "order-created-topic", orderId.toString(), event);

        List<OutboxEventEntity> rows = outboxRepository.findAll();
        assertThat(rows).hasSize(1);
        OutboxEventEntity row = rows.get(0);
        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.getEventType()).isEqualTo("OrderCreated");
        assertThat(row.getTopic()).isEqualTo("order-created-topic");
        assertThat(row.getMessageKey()).isEqualTo(orderId.toString());
        assertThat(row.getAttempts()).isZero();
        assertThat(row.getPublishedAt()).isNull();
        assertThat(row.getPayload())
                .contains(orderId.toString())
                .contains(event.eventId().toString())
                .contains("\"sku\":\"SKU-1\"")
                .contains("49.50");
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void refusesToAppendOutsideABusinessTransaction() {
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = OrderCreatedEvent.of(orderId, "SKU-1", 1, BigDecimal.ONE);

        // The whole point of an outbox is that the event commits with the business change. Appending
        // without a surrounding transaction would quietly reintroduce the dual-write problem.
        assertThatThrownBy(() -> outboxService.append(event.eventId(), "Order", orderId, "OrderCreated",
                "order-created-topic", orderId.toString(), event))
                .isInstanceOf(IllegalTransactionStateException.class);
    }
}
