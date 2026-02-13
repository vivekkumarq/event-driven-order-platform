package com.vivek.platform.order.service;

import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import com.vivek.platform.order.events.OrderCreatedEvent;
import com.vivek.platform.order.messaging.OrderEventProducer;
import com.vivek.platform.order.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventProducer eventProducer;

    public OrderService(OrderRepository orderRepository, OrderEventProducer eventProducer) {
        this.orderRepository = orderRepository;
        this.eventProducer = eventProducer;
    }

    public OrderEntity createOrder(Double amount) {
        OrderEntity order = new OrderEntity();
        order.setAmount(amount);
        order.setStatus(OrderStatus.CREATED);
        order.setCreatedAt(Instant.now());

        OrderEntity saved = orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent();
        event.setOrderId(saved.getId());
        event.setAmount(saved.getAmount());

        eventProducer.publishOrderCreated(event);

        return saved;
    }

    public OrderEntity getOrder(UUID id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Order not found: " + id));
    }
}
