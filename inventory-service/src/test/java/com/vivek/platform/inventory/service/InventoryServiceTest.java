package com.vivek.platform.inventory.service;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import com.vivek.platform.inventory.exception.DuplicateSkuException;
import com.vivek.platform.inventory.exception.InventoryItemNotFoundException;
import com.vivek.platform.inventory.repository.InventoryItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class InventoryServiceTest {

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private InventoryItemRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void createsAndReadsBackAnItem() {
        inventoryService.createItem("SKU-1", 42);

        InventoryItemEntity found = inventoryService.getBySku("SKU-1");
        assertThat(found.getAvailableQuantity()).isEqualTo(42);
        assertThat(found.getReservedQuantity()).isZero();
        assertThat(found.getVersion()).isNotNull();
    }

    @Test
    void refusesADuplicateSku() {
        inventoryService.createItem("SKU-1", 1);

        assertThatThrownBy(() -> inventoryService.createItem("SKU-1", 2))
                .isInstanceOf(DuplicateSkuException.class);
    }

    @Test
    void reportsAnUnknownSku() {
        assertThatThrownBy(() -> inventoryService.getBySku("SKU-NOPE"))
                .isInstanceOf(InventoryItemNotFoundException.class);
    }

    @Test
    @DisplayName("updating the stock level leaves reserved units alone")
    void updatingAvailableStockDoesNotDisturbReservations() {
        InventoryItemEntity item = repository.save(new InventoryItemEntity("SKU-1", 10));
        item.reserve(4);
        repository.save(item);

        inventoryService.updateAvailableQuantity("SKU-1", 100);

        InventoryItemEntity reloaded = inventoryService.getBySku("SKU-1");
        assertThat(reloaded.getAvailableQuantity()).isEqualTo(100);
        assertThat(reloaded.getReservedQuantity()).isEqualTo(4);
    }

    @Test
    void listsItemsBySku() {
        inventoryService.createItem("SKU-B", 1);
        inventoryService.createItem("SKU-A", 2);

        assertThat(inventoryService.listItems())
                .extracting(InventoryItemEntity::getSku)
                .containsExactly("SKU-A", "SKU-B");
    }

    @Test
    void deletesAnItem() {
        inventoryService.createItem("SKU-1", 1);

        inventoryService.deleteItem("SKU-1");

        assertThat(repository.existsBySku("SKU-1")).isFalse();
        assertThatThrownBy(() -> inventoryService.deleteItem("SKU-1"))
                .isInstanceOf(InventoryItemNotFoundException.class);
    }
}
