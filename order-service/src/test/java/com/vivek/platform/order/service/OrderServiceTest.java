package com.vivek.platform.order.service;

import com.vivek.platform.order.TestOrders;
import com.vivek.platform.order.api.dto.CreateOrderRequest;
import com.vivek.platform.order.config.KafkaTopicsProperties;
import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import com.vivek.platform.order.domain.ProcessedEventEntity;
import com.vivek.platform.order.events.InventoryReservationResultEvent;
import com.vivek.platform.order.events.OrderCancelledEvent;
import com.vivek.platform.order.events.OrderCreatedEvent;
import com.vivek.platform.order.events.ReservationStatus;
import com.vivek.platform.order.exception.IllegalOrderTransitionException;
import com.vivek.platform.order.exception.OrderNotFoundException;
import com.vivek.platform.order.repository.OrderRepository;
import com.vivek.platform.order.repository.OutboxEventRepository;
import com.vivek.platform.order.repository.ProcessedEventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final KafkaTopicsProperties TOPICS = new KafkaTopicsProperties(
            "order-created-topic", "order-cancelled-topic", "inventory-reservation-result-topic");

    @Mock
    private OrderRepository orderRepository;
    @Mock
    private ProcessedEventRepository processedEventRepository;
    @Mock
    private OutboxService outboxService;
    @Mock
    private OutboxEventRepository outboxEventRepository;

    private OrderService orderService;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        OrderMetrics metrics = new OrderMetrics(meterRegistry, outboxEventRepository);
        orderService = new OrderService(orderRepository, processedEventRepository, outboxService,
                TOPICS, metrics);
    }

    private double counter(String name) {
        return meterRegistry.get(name).counter().count();
    }

    @Nested
    class CreateOrder {

        @Test
        @DisplayName("persists a PENDING order and queues its event in the same call")
        void createsPendingOrderAndOutboxEvent() {
            UUID id = UUID.randomUUID();
            when(orderRepository.save(any(OrderEntity.class)))
                    .thenAnswer(invocation -> TestOrders.pending(id, "SKU-1", 3, "1499.99"));

            OrderEntity created = orderService.createOrder(
                    new CreateOrderRequest("SKU-1", 3, new BigDecimal("1499.99")));

            assertThat(created.getStatus()).isEqualTo(OrderStatus.PENDING);
            assertThat(created.getAmount()).isEqualByComparingTo("1499.99");

            ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
            verify(outboxService).append(any(UUID.class), eq("Order"), eq(id), eq("OrderCreated"),
                    eq("order-created-topic"), eq(id.toString()), payload.capture());

            assertThat(payload.getValue()).isInstanceOfSatisfying(OrderCreatedEvent.class, event -> {
                assertThat(event.orderId()).isEqualTo(id);
                assertThat(event.sku()).isEqualTo("SKU-1");
                assertThat(event.quantity()).isEqualTo(3);
                assertThat(event.amount()).isEqualByComparingTo("1499.99");
                assertThat(event.eventId()).isNotNull();
            });
            assertThat(counter("platform.orders.created")).isEqualTo(1);
        }
    }

    @Nested
    class GetOrder {

        @Test
        void throwsANotFoundExceptionRatherThanABareRuntimeException() {
            UUID id = UUID.randomUUID();
            when(orderRepository.findById(id)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orderService.getOrder(id))
                    .isInstanceOf(OrderNotFoundException.class)
                    .hasMessageContaining(id.toString());
        }
    }

    @Nested
    class CancelOrder {

        @Test
        @DisplayName("cancelling a PENDING order releases nothing, because nothing is reserved yet")
        void cancellingPendingOrderQueuesNoCompensation() {
            UUID id = UUID.randomUUID();
            when(orderRepository.findById(id)).thenReturn(Optional.of(TestOrders.pending(id)));

            OrderEntity cancelled = orderService.cancelOrder(id, "changed my mind");

            assertThat(cancelled.getStatus()).isEqualTo(OrderStatus.CANCELLED);
            assertThat(cancelled.getStatusReason()).isEqualTo("changed my mind");
            verifyNoInteractions(outboxService);
            assertThat(counter("platform.orders.cancelled")).isEqualTo(1);
        }

        @Test
        @DisplayName("cancelling a CONFIRMED order queues a compensating OrderCancelled event")
        void cancellingConfirmedOrderQueuesCompensation() {
            UUID id = UUID.randomUUID();
            when(orderRepository.findById(id))
                    .thenReturn(Optional.of(TestOrders.inStatus(id, OrderStatus.CONFIRMED)));

            orderService.cancelOrder(id, "customer request");

            ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
            verify(outboxService).append(any(UUID.class), eq("Order"), eq(id), eq("OrderCancelled"),
                    eq("order-cancelled-topic"), eq(id.toString()), payload.capture());
            assertThat(payload.getValue()).isInstanceOfSatisfying(OrderCancelledEvent.class, event -> {
                assertThat(event.orderId()).isEqualTo(id);
                assertThat(event.quantity()).isEqualTo(2);
                assertThat(event.reason()).isEqualTo("customer request");
            });
        }

        @Test
        void refusesToCancelATerminalOrder() {
            UUID id = UUID.randomUUID();
            when(orderRepository.findById(id))
                    .thenReturn(Optional.of(TestOrders.inStatus(id, OrderStatus.REJECTED)));

            assertThatThrownBy(() -> orderService.cancelOrder(id, null))
                    .isInstanceOf(IllegalOrderTransitionException.class)
                    .hasMessageContaining("REJECTED -> CANCELLED");
            verifyNoInteractions(outboxService);
        }

        @Test
        void reportsAMissingOrder() {
            UUID id = UUID.randomUUID();
            when(orderRepository.findById(id)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orderService.cancelOrder(id, null))
                    .isInstanceOf(OrderNotFoundException.class);
        }
    }

    @Nested
    class ApplyReservationResult {

        @Test
        void confirmsThePendingOrderOnASuccessfulReservation() {
            UUID id = UUID.randomUUID();
            OrderEntity order = TestOrders.pending(id);
            when(processedEventRepository.existsById(any(UUID.class))).thenReturn(false);
            when(orderRepository.findById(id)).thenReturn(Optional.of(order));

            orderService.applyReservationResult(result(id, ReservationStatus.RESERVED, null));

            assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
            verify(processedEventRepository).save(any(ProcessedEventEntity.class));
            assertThat(counter("platform.orders.confirmed")).isEqualTo(1);
        }

        @Test
        void rejectsThePendingOrderAndKeepsTheReason() {
            UUID id = UUID.randomUUID();
            OrderEntity order = TestOrders.pending(id);
            when(processedEventRepository.existsById(any(UUID.class))).thenReturn(false);
            when(orderRepository.findById(id)).thenReturn(Optional.of(order));

            orderService.applyReservationResult(
                    result(id, ReservationStatus.FAILED, "Insufficient stock for SKU SKU-1"));

            assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
            assertThat(order.getStatusReason()).isEqualTo("Insufficient stock for SKU SKU-1");
            assertThat(counter("platform.orders.rejected")).isEqualTo(1);
        }

        @Test
        @DisplayName("a redelivered result is skipped, so the order is not touched twice")
        void skipsAlreadyProcessedEvents() {
            UUID id = UUID.randomUUID();
            when(processedEventRepository.existsById(any(UUID.class))).thenReturn(true);

            orderService.applyReservationResult(result(id, ReservationStatus.RESERVED, null));

            verify(orderRepository, never()).findById(any(UUID.class));
            verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
            assertThat(counter("platform.events.duplicates.skipped")).isEqualTo(1);
            assertThat(counter("platform.orders.confirmed")).isZero();
        }

        @Test
        @DisplayName("a reservation that lands after cancellation is compensated, not applied")
        void compensatesWhenTheOrderWasCancelledFirst() {
            UUID id = UUID.randomUUID();
            OrderEntity order = TestOrders.inStatus(id, OrderStatus.CANCELLED);
            when(processedEventRepository.existsById(any(UUID.class))).thenReturn(false);
            when(orderRepository.findById(id)).thenReturn(Optional.of(order));

            orderService.applyReservationResult(result(id, ReservationStatus.RESERVED, null));

            assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
            verify(outboxService).append(any(UUID.class), eq("Order"), eq(id), eq("OrderCancelled"),
                    eq("order-cancelled-topic"), eq(id.toString()), any());
        }

        @Test
        void toleratesAResultForAnUnknownOrder() {
            UUID id = UUID.randomUUID();
            when(processedEventRepository.existsById(any(UUID.class))).thenReturn(false);
            when(orderRepository.findById(id)).thenReturn(Optional.empty());

            orderService.applyReservationResult(result(id, ReservationStatus.RESERVED, null));

            verify(orderRepository, never()).save(any(OrderEntity.class));
            verify(outboxService, never()).append(any(), anyString(), any(), anyString(), anyString(),
                    anyString(), any());
        }

        private InventoryReservationResultEvent result(UUID orderId, ReservationStatus status,
                                                       String reason) {
            return new InventoryReservationResultEvent(UUID.randomUUID(), orderId, "SKU-1", 2,
                    status, reason, Instant.now());
        }
    }
}
