package com.vivek.platform.order.messaging;

import com.vivek.platform.order.config.OutboxProperties;
import com.vivek.platform.order.domain.OutboxEventEntity;
import com.vivek.platform.order.domain.OutboxStatus;
import com.vivek.platform.order.repository.OutboxEventRepository;
import com.vivek.platform.order.service.OrderMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Moves committed outbox rows onto Kafka.
 *
 * <p>Publishing is deliberately synchronous: the row is only marked PUBLISHED once the broker has
 * acknowledged it, so a crash mid-flight leaves the row PENDING and the event is re-sent. That
 * makes delivery at-least-once, which is exactly why consumers de-duplicate on {@code eventId}.
 *
 * <p><strong>Known limitation:</strong> rows are claimed without a database lock. Spring's default
 * scheduler is single-threaded and {@code fixedDelay} prevents overlapping runs, so one instance is
 * safe; running several instances would need {@code SELECT ... FOR UPDATE SKIP LOCKED} to stop two
 * relays publishing the same row.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties properties;
    private final OrderMetrics metrics;

    public OutboxRelay(OutboxEventRepository outboxRepository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       OutboxProperties properties,
                       OrderMetrics metrics) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${platform.outbox.poll-interval-ms}")
    public void scheduledRelay() {
        if (!properties.enabled()) {
            return;
        }
        relayPendingEvents();
    }

    /**
     * Publishes one batch of pending rows.
     *
     * @return the number of rows successfully published
     */
    public int relayPendingEvents() {
        List<OutboxEventEntity> pending = outboxRepository.findByStatusOrderByCreatedAtAsc(
                OutboxStatus.PENDING, Limit.of(properties.batchSize()));
        if (pending.isEmpty()) {
            return 0;
        }

        int published = 0;
        for (OutboxEventEntity event : pending) {
            if (publish(event)) {
                published++;
            }
        }
        log.debug("Outbox relay published {}/{} pending events", published, pending.size());
        return published;
    }

    private boolean publish(OutboxEventEntity event) {
        try {
            kafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload())
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            event.markPublished();
            outboxRepository.save(event);
            metrics.outboxPublished();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            recordFailure(event, e);
            return false;
        } catch (Exception e) {
            recordFailure(event, e);
            return false;
        }
    }

    private void recordFailure(OutboxEventEntity event, Exception cause) {
        event.markAttemptFailed(cause.toString(), properties.maxAttempts());
        outboxRepository.save(event);
        metrics.outboxPublishFailed();
        if (event.getStatus() == OutboxStatus.FAILED) {
            log.error("Outbox event {} ({}) parked as FAILED after {} attempts",
                    event.getEventId(), event.getEventType(), event.getAttempts(), cause);
        } else {
            log.warn("Outbox event {} ({}) publish attempt {} failed; will retry",
                    event.getEventId(), event.getEventType(), event.getAttempts(), cause);
        }
    }
}
