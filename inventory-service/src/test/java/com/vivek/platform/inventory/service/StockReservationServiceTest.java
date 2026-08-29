package com.vivek.platform.inventory.service;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import com.vivek.platform.inventory.domain.StockReservationEntity;
import com.vivek.platform.inventory.events.OrderCancelledEvent;
import com.vivek.platform.inventory.events.OrderCreatedEvent;
import com.vivek.platform.inventory.events.ReservationStatus;
import com.vivek.platform.inventory.repository.InventoryItemRepository;
import com.vivek.platform.inventory.repository.ProcessedEventRepository;
import com.vivek.platform.inventory.repository.StockReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reservation behaviour against a real database. No broker is involved: this class exercises the
 * transactional core, which is deliberately separate from publishing.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class StockReservationServiceTest {

    @Autowired
    private StockReservationService reservationService;
    @Autowired
    private InventoryItemRepository itemRepository;
    @Autowired
    private StockReservationRepository reservationRepository;
    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @BeforeEach
    void setUp() {
        reservationRepository.deleteAll();
        processedEventRepository.deleteAll();
        itemRepository.deleteAll();
    }

    private void stock(String sku, int quantity) {
        itemRepository.save(new InventoryItemEntity(sku, quantity));
    }

    private OrderCreatedEvent orderFor(String sku, int quantity) {
        return OrderCreatedEvent.of(UUID.randomUUID(), sku, quantity, new BigDecimal("10.00"));
    }

    private InventoryItemEntity reload(String sku) {
        return itemRepository.findBySku(sku).orElseThrow();
    }

    @Test
    void reservesStockWhenThereIsEnough() {
        stock("SKU-1", 10);
        OrderCreatedEvent event = orderFor("SKU-1", 4);

        StockReservationEntity reservation = reservationService.reserve(event);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RESERVED);
        assertThat(reservation.getReason()).isNull();
        assertThat(reservation.getResultEventId()).isNotNull();
        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(6);
        assertThat(reload("SKU-1").getReservedQuantity()).isEqualTo(4);
    }

    @Test
    @DisplayName("insufficient stock is refused with a reason, and no units move")
    void refusesWhenStockIsInsufficient() {
        stock("SKU-1", 3);
        OrderCreatedEvent event = orderFor("SKU-1", 5);

        StockReservationEntity reservation = reservationService.reserve(event);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.FAILED);
        assertThat(reservation.getReason())
                .contains("Insufficient stock")
                .contains("requested 5")
                .contains("available 3");
        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(3);
        assertThat(reload("SKU-1").getReservedQuantity()).isZero();
    }

    @Test
    void refusesAnUnknownSku() {
        StockReservationEntity reservation = reservationService.reserve(orderFor("SKU-NOPE", 1));

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.FAILED);
        assertThat(reservation.getReason()).isEqualTo("Unknown SKU: SKU-NOPE");
    }

    @Test
    @DisplayName("the same event delivered twice reserves once")
    void isIdempotentOnEventId() {
        stock("SKU-1", 10);
        OrderCreatedEvent event = orderFor("SKU-1", 4);

        StockReservationEntity first = reservationService.reserve(event);
        StockReservationEntity second = reservationService.reserve(event);

        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(6);
        assertThat(reload("SKU-1").getReservedQuantity()).isEqualTo(4);
        assertThat(reservationRepository.count()).isEqualTo(1);
        // The stable result id is what lets order-service de-duplicate the re-publish too.
        assertThat(second.getResultEventId()).isEqualTo(first.getResultEventId());
    }

    @Test
    @DisplayName("a redelivered failure is not re-evaluated against newer stock")
    void isIdempotentForFailuresToo() {
        stock("SKU-1", 1);
        OrderCreatedEvent event = orderFor("SKU-1", 5);
        StockReservationEntity first = reservationService.reserve(event);
        assertThat(first.getStatus()).isEqualTo(ReservationStatus.FAILED);

        InventoryItemEntity restocked = reload("SKU-1");
        restocked.setAvailableQuantity(100);
        itemRepository.save(restocked);

        StockReservationEntity second = reservationService.reserve(event);

        assertThat(second.getStatus()).isEqualTo(ReservationStatus.FAILED);
        assertThat(reload("SKU-1").getReservedQuantity()).isZero();
    }

    @Test
    void releasesStockHeldForACancelledOrder() {
        stock("SKU-1", 10);
        OrderCreatedEvent created = orderFor("SKU-1", 4);
        reservationService.reserve(created);

        reservationService.release(OrderCancelledEvent.of(created.orderId(), "SKU-1", 4, "cancelled"));

        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(10);
        assertThat(reload("SKU-1").getReservedQuantity()).isZero();
        assertThat(reservationRepository.findByOrderId(created.orderId()).orElseThrow().isReleased())
                .isTrue();
    }

    @Test
    @DisplayName("a redelivered cancellation does not return the units twice")
    void releaseIsIdempotentOnEventId() {
        stock("SKU-1", 10);
        OrderCreatedEvent created = orderFor("SKU-1", 4);
        reservationService.reserve(created);
        OrderCancelledEvent cancelled = OrderCancelledEvent.of(created.orderId(), "SKU-1", 4, "cancelled");

        reservationService.release(cancelled);
        reservationService.release(cancelled);

        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(10);
        assertThat(reload("SKU-1").getTotalQuantity()).isEqualTo(10);
        assertThat(processedEventRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("two different cancellations for the same order still release only once")
    void releaseIsIdempotentOnTheReservationToo() {
        stock("SKU-1", 10);
        OrderCreatedEvent created = orderFor("SKU-1", 4);
        reservationService.reserve(created);

        reservationService.release(OrderCancelledEvent.of(created.orderId(), "SKU-1", 4, "first"));
        reservationService.release(OrderCancelledEvent.of(created.orderId(), "SKU-1", 4, "second"));

        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(10);
        assertThat(reload("SKU-1").getTotalQuantity()).isEqualTo(10);
    }

    @Test
    void cancellingAnOrderThatNeverReservedIsANoOp() {
        stock("SKU-1", 10);

        reservationService.release(OrderCancelledEvent.of(UUID.randomUUID(), "SKU-1", 4, "cancelled"));

        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(10);
    }

    @Test
    void cancellingAnOrderWhoseReservationFailedIsANoOp() {
        stock("SKU-1", 1);
        OrderCreatedEvent created = orderFor("SKU-1", 5);
        reservationService.reserve(created);

        reservationService.release(OrderCancelledEvent.of(created.orderId(), "SKU-1", 5, "cancelled"));

        assertThat(reload("SKU-1").getAvailableQuantity()).isEqualTo(1);
        assertThat(reload("SKU-1").getReservedQuantity()).isZero();
    }
}
