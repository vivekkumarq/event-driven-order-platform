package com.vivek.platform.order.repository;

import com.vivek.platform.order.domain.OutboxEventEntity;
import com.vivek.platform.order.domain.OutboxStatus;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    /** Oldest-first batch of rows the relay still has to publish. */
    List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(OutboxStatus status, Limit limit);

    long countByStatus(OutboxStatus status);

    List<OutboxEventEntity> findByAggregateIdOrderByCreatedAtAsc(UUID aggregateId);
}
