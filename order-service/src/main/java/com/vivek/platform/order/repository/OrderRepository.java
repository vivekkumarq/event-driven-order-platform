package com.vivek.platform.order.repository;

import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {

    List<OrderEntity> findByStatusOrderByCreatedAtDesc(OrderStatus status);

    List<OrderEntity> findAllByOrderByCreatedAtDesc();
}
