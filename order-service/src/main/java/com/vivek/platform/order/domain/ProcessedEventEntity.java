package com.vivek.platform.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * Inbox / de-duplication record.
 *
 * <p>Kafka guarantees at-least-once delivery, so a consumer must expect the same event twice. Every
 * consumed event id is written here inside the handling transaction; a redelivery of an id that is
 * already present is skipped, which makes consumption effectively idempotent.
 *
 * <p>The {@link Persistable} implementation is load-bearing, not decoration. The id is assigned by
 * the caller rather than generated, so Spring Data would otherwise treat every instance as an
 * existing row and call {@code merge}, which silently overwrites a duplicate instead of rejecting
 * it. Forcing {@code persist} means two consumers racing on the same event id collide on the
 * primary key, one transaction rolls back, and its redelivery finds the row and skips.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEventEntity implements Persistable<UUID> {

    @Id
    private UUID eventId;

    @Column(nullable = false, length = 128)
    private String listener;

    @Column(nullable = false)
    private Instant processedAt;

    @Transient
    private boolean newRecord;

    protected ProcessedEventEntity() {
        // for JPA
    }

    public ProcessedEventEntity(UUID eventId, String listener) {
        this.eventId = eventId;
        this.listener = listener;
        this.processedAt = Instant.now();
        this.newRecord = true;
    }

    @PostPersist
    @PostLoad
    void markNotNew() {
        this.newRecord = false;
    }

    @Override
    public UUID getId() {
        return eventId;
    }

    @Override
    public boolean isNew() {
        return newRecord;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getListener() {
        return listener;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
