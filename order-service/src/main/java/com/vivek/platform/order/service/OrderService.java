package com.vivek.platform.order.service;

import com.vivek.platform.order.api.dto.CreateOrderRequest;
import com.vivek.platform.order.config.KafkaTopicsProperties;
import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import com.vivek.platform.order.domain.ProcessedEventEntity;
import com.vivek.platform.order.events.InventoryReservationResultEvent;
import com.vivek.platform.order.events.OrderCancelledEvent;
import com.vivek.platform.order.events.OrderCreatedEvent;
import com.vivek.platform.order.exception.IllegalOrderTransitionException;
import com.vivek.platform.order.exception.OrderNotFoundException;
import com.vivek.platform.order.repository.OrderRepository;
import com.vivek.platform.order.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Order lifecycle and the order side of the reservation saga.
 *
 * <p>Every write path is transactional and appends its outgoing event to the outbox in the same
 * transaction, so an order is never persisted without its event or vice versa.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String AGGREGATE_TYPE = "Order";
    private static final String LISTENER = "inventory-reservation-result";

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxService outboxService;
    private final KafkaTopicsProperties topics;
    private final OrderMetrics metrics;

    public OrderService(OrderRepository orderRepository,
                        ProcessedEventRepository processedEventRepository,
                        OutboxService outboxService,
                        KafkaTopicsProperties topics,
                        OrderMetrics metrics) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxService = outboxService;
        this.topics = topics;
        this.metrics = metrics;
    }

    /**
     * Persists a new PENDING order and its OrderCreated event atomically.
     */
    @Transactional
    public OrderEntity createOrder(CreateOrderRequest request) {
        OrderEntity order = orderRepository.save(
                new OrderEntity(request.sku(), request.quantity(), request.amount()));

        OrderCreatedEvent event = OrderCreatedEvent.of(
                order.getId(), order.getSku(), order.getQuantity(), order.getAmount());
        outboxService.append(event.eventId(), AGGREGATE_TYPE, order.getId(), "OrderCreated",
                topics.orderCreated(), order.getId().toString(), event);

        metrics.orderCreated();
        log.info("Created order {} sku={} quantity={} amount={}",
                order.getId(), order.getSku(), order.getQuantity(), order.getAmount());
        return order;
    }

    @Transactional(readOnly = true)
    public OrderEntity getOrder(UUID id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public List<OrderEntity> listOrders(OrderStatus status) {
        return status == null
                ? orderRepository.findAllByOrderByCreatedAtDesc()
                : orderRepository.findByStatusOrderByCreatedAtDesc(status);
    }

    /**
     * Cancels an order. When the order was already CONFIRMED the reserved stock is still held by
     * inventory-service, so a compensating OrderCancelled event is queued to release it.
     */
    @Transactional
    public OrderEntity cancelOrder(UUID id, String reason) {
        OrderEntity order = orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
        OrderStatus previous = order.getStatus();
        if (!previous.canTransitionTo(OrderStatus.CANCELLED)) {
            throw new IllegalOrderTransitionException(previous, OrderStatus.CANCELLED);
        }

        order.transitionTo(OrderStatus.CANCELLED, reason);
        orderRepository.save(order);

        if (previous == OrderStatus.CONFIRMED) {
            queueStockRelease(order, reason == null ? "Order cancelled" : reason);
        }

        metrics.orderCancelled();
        log.info("Cancelled order {} (was {})", id, previous);
        return order;
    }

    /**
     * Applies a reservation result from inventory-service.
     *
     * <p>De-duplicated on {@code eventId}: Kafka redelivers, and applying the same result twice must
     * not double-count metrics or resurrect a cancelled order. If the two workers race, the primary
     * key on {@code processed_events} rejects the loser, its transaction rolls back and the retry
     * sees the row on the next attempt.
     */
    @Transactional
    public void applyReservationResult(InventoryReservationResultEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            metrics.duplicateSkipped();
            log.debug("Skipping already-processed reservation result {}", event.eventId());
            return;
        }
        processedEventRepository.save(new ProcessedEventEntity(event.eventId(), LISTENER));

        Optional<OrderEntity> found = orderRepository.findById(event.orderId());
        if (found.isEmpty()) {
            log.warn("Reservation result {} refers to unknown order {}", event.eventId(), event.orderId());
            return;
        }
        OrderEntity order = found.get();

        if (order.getStatus() == OrderStatus.PENDING) {
            if (event.isReserved()) {
                order.transitionTo(OrderStatus.CONFIRMED, null);
                metrics.orderConfirmed();
            } else {
                order.transitionTo(OrderStatus.REJECTED, event.reason());
                metrics.orderRejected();
            }
            orderRepository.save(order);
            log.info("Order {} moved to {} by reservation result {}",
                    order.getId(), order.getStatus(), event.eventId());
            return;
        }

        if (order.getStatus() == OrderStatus.CANCELLED && event.isReserved()) {
            // The caller cancelled while the reservation was in flight, so stock was reserved for an
            // order that no longer exists. Compensate rather than leak the units.
            queueStockRelease(order, "Order cancelled before reservation result arrived");
            log.info("Order {} was cancelled before its reservation landed; releasing stock", order.getId());
            return;
        }

        log.warn("Ignoring reservation result {} for order {} in terminal state {}",
                event.eventId(), order.getId(), order.getStatus());
    }

    private void queueStockRelease(OrderEntity order, String reason) {
        OrderCancelledEvent cancelled = OrderCancelledEvent.of(
                order.getId(), order.getSku(), order.getQuantity(), reason);
        outboxService.append(cancelled.eventId(), AGGREGATE_TYPE, order.getId(), "OrderCancelled",
                topics.orderCancelled(), order.getId().toString(), cancelled);
    }
}
