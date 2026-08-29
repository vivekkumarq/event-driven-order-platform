package com.vivek.platform.order.service;

import com.vivek.platform.order.domain.OutboxStatus;
import com.vivek.platform.order.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Custom application metrics, exported on {@code /actuator/prometheus}.
 *
 * <p>The outbox backlog gauge runs a {@code count} query per scrape; on a large table you would
 * cache it or maintain the figure incrementally.
 */
@Component
public class OrderMetrics {

    private final Counter ordersCreated;
    private final Counter ordersConfirmed;
    private final Counter ordersRejected;
    private final Counter ordersCancelled;
    private final Counter outboxPublished;
    private final Counter outboxPublishFailures;
    private final Counter duplicateEventsSkipped;

    public OrderMetrics(MeterRegistry registry, OutboxEventRepository outboxRepository) {
        this.ordersCreated = Counter.builder("platform.orders.created")
                .description("Orders accepted by the API").register(registry);
        this.ordersConfirmed = Counter.builder("platform.orders.confirmed")
                .description("Orders confirmed after a successful stock reservation").register(registry);
        this.ordersRejected = Counter.builder("platform.orders.rejected")
                .description("Orders rejected because stock could not be reserved").register(registry);
        this.ordersCancelled = Counter.builder("platform.orders.cancelled")
                .description("Orders cancelled by a caller").register(registry);
        this.outboxPublished = Counter.builder("platform.outbox.published")
                .description("Outbox rows relayed to Kafka").register(registry);
        this.outboxPublishFailures = Counter.builder("platform.outbox.publish.failures")
                .description("Failed outbox publish attempts").register(registry);
        this.duplicateEventsSkipped = Counter.builder("platform.events.duplicates.skipped")
                .description("Redelivered events skipped by the de-duplication check").register(registry);

        Gauge.builder("platform.outbox.pending", outboxRepository,
                        repository -> repository.countByStatus(OutboxStatus.PENDING))
                .description("Outbox rows waiting to be relayed").register(registry);
    }

    public void orderCreated() {
        ordersCreated.increment();
    }

    public void orderConfirmed() {
        ordersConfirmed.increment();
    }

    public void orderRejected() {
        ordersRejected.increment();
    }

    public void orderCancelled() {
        ordersCancelled.increment();
    }

    public void outboxPublished() {
        outboxPublished.increment();
    }

    public void outboxPublishFailed() {
        outboxPublishFailures.increment();
    }

    public void duplicateSkipped() {
        duplicateEventsSkipped.increment();
    }
}
