package com.vivek.platform.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.platform.order.domain.OutboxEventEntity;
import com.vivek.platform.order.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Appends events to the transactional outbox.
 *
 * <p>{@link Propagation#MANDATORY} is deliberate: appending an event only means anything when it
 * joins the caller's business transaction. Calling this outside a transaction is a programming
 * error and fails loudly rather than silently reintroducing the dual-write problem.
 */
@Service
public class OutboxService {

    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxEventRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEventEntity append(UUID eventId, String aggregateType, UUID aggregateId,
                                    String eventType, String topic, String messageKey, Object payload) {
        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            // Rolls back the business transaction: an order we cannot announce must not be stored.
            throw new IllegalStateException("Unable to serialise outbox payload for " + eventType, e);
        }
        return outboxRepository.save(new OutboxEventEntity(
                eventId, aggregateType, aggregateId, eventType, topic, messageKey, json));
    }
}
