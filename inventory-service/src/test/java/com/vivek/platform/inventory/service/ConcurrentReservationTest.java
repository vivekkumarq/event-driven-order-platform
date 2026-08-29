package com.vivek.platform.inventory.service;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import com.vivek.platform.inventory.domain.StockReservationEntity;
import com.vivek.platform.inventory.events.OrderCreatedEvent;
import com.vivek.platform.inventory.events.ReservationStatus;
import com.vivek.platform.inventory.messaging.InventoryEventPublisher;
import com.vivek.platform.inventory.repository.InventoryItemRepository;
import com.vivek.platform.inventory.repository.ProcessedEventRepository;
import com.vivek.platform.inventory.repository.StockReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent reservation against one SKU.
 *
 * <p>This is the test the optimistic {@code @Version} column exists for. Without it two consumers
 * reading the same row would both write their own view back, the second overwriting the first, and
 * the SKU would be oversold with no error anywhere. Here the collision is detected and retried
 * against freshly read state, so the arithmetic still adds up under contention.
 *
 * <p>The publisher is mocked: this is about the database, not the broker.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "platform.inventory.reservation.max-attempts=50",
        "platform.inventory.reservation.backoff-ms=5"
})
@ActiveProfiles("test")
class ConcurrentReservationTest {

    private static final int THREADS = 8;

    @Autowired
    private ReservationCoordinator coordinator;
    @Autowired
    private InventoryItemRepository itemRepository;
    @Autowired
    private StockReservationRepository reservationRepository;
    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @MockitoBean
    private InventoryEventPublisher publisher;

    @BeforeEach
    void setUp() {
        reservationRepository.deleteAll();
        processedEventRepository.deleteAll();
        itemRepository.deleteAll();
    }

    private List<StockReservationEntity> reserveConcurrently(String sku, int perOrder, int orders)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            CountDownLatch startLine = new CountDownLatch(1);
            List<Callable<StockReservationEntity>> tasks = new ArrayList<>();
            for (int i = 0; i < orders; i++) {
                tasks.add(() -> {
                    startLine.await();
                    return coordinator.onOrderCreated(OrderCreatedEvent.of(
                            UUID.randomUUID(), sku, perOrder, new BigDecimal("10.00")));
                });
            }
            List<Future<StockReservationEntity>> futures = new ArrayList<>();
            for (Callable<StockReservationEntity> task : tasks) {
                futures.add(pool.submit(task));
            }
            startLine.countDown();

            List<StockReservationEntity> results = new ArrayList<>();
            for (Future<StockReservationEntity> future : futures) {
                results.add(future.get(60, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("concurrent reservations that all fit are all applied, and none are lost")
    void appliesEveryConcurrentReservationWhenStockIsAmple() throws Exception {
        itemRepository.save(new InventoryItemEntity("SKU-1", 100));

        List<StockReservationEntity> results = reserveConcurrently("SKU-1", 2, 20);

        assertThat(results).allMatch(StockReservationEntity::isReserved);
        InventoryItemEntity item = itemRepository.findBySku("SKU-1").orElseThrow();
        assertThat(item.getReservedQuantity()).isEqualTo(40);
        assertThat(item.getAvailableQuantity()).isEqualTo(60);
        assertThat(item.getTotalQuantity()).isEqualTo(100);
    }

    @Test
    @DisplayName("contention for the last few units never oversells")
    void neverOversellsUnderContention() throws Exception {
        itemRepository.save(new InventoryItemEntity("SKU-SCARCE", 5));

        List<StockReservationEntity> results = reserveConcurrently("SKU-SCARCE", 1, 20);

        long reserved = results.stream().filter(StockReservationEntity::isReserved).count();
        long failed = results.stream()
                .filter(r -> r.getStatus() == ReservationStatus.FAILED).count();

        assertThat(reserved).isEqualTo(5);
        assertThat(failed).isEqualTo(15);

        InventoryItemEntity item = itemRepository.findBySku("SKU-SCARCE").orElseThrow();
        assertThat(item.getAvailableQuantity()).isZero();
        assertThat(item.getReservedQuantity()).isEqualTo(5);
        assertThat(item.getTotalQuantity()).isEqualTo(5);
    }

    @Test
    @DisplayName("the same event delivered concurrently reserves exactly once")
    void concurrentRedeliveryOfOneEventReservesOnce() throws Exception {
        itemRepository.save(new InventoryItemEntity("SKU-1", 50));
        OrderCreatedEvent event = OrderCreatedEvent.of(
                UUID.randomUUID(), "SKU-1", 3, new BigDecimal("10.00"));

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            CountDownLatch startLine = new CountDownLatch(1);
            List<Future<StockReservationEntity>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                futures.add(pool.submit(() -> {
                    startLine.await();
                    return coordinator.onOrderCreated(event);
                }));
            }
            startLine.countDown();
            for (Future<StockReservationEntity> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        InventoryItemEntity item = itemRepository.findBySku("SKU-1").orElseThrow();
        assertThat(item.getReservedQuantity()).isEqualTo(3);
        assertThat(item.getAvailableQuantity()).isEqualTo(47);
        assertThat(reservationRepository.count()).isEqualTo(1);
    }
}
