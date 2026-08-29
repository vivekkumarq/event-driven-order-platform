package com.vivek.platform.inventory.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InventoryItemEntityTest {

    @Test
    void reservingMovesUnitsFromAvailableToReserved() {
        InventoryItemEntity item = new InventoryItemEntity("SKU-1", 10);

        item.reserve(4);

        assertThat(item.getAvailableQuantity()).isEqualTo(6);
        assertThat(item.getReservedQuantity()).isEqualTo(4);
        assertThat(item.getTotalQuantity()).isEqualTo(10);
    }

    @Test
    @DisplayName("physical stock is conserved by a reserve/release round trip")
    void releasingReturnsUnitsToAvailable() {
        InventoryItemEntity item = new InventoryItemEntity("SKU-1", 10);
        item.reserve(4);

        item.release(4);

        assertThat(item.getAvailableQuantity()).isEqualTo(10);
        assertThat(item.getReservedQuantity()).isZero();
    }

    @Test
    @DisplayName("a duplicate release cannot push the reserved bucket negative")
    void releaseIsClampedToWhatIsActuallyReserved() {
        InventoryItemEntity item = new InventoryItemEntity("SKU-1", 10);
        item.reserve(2);

        item.release(2);
        item.release(2);

        assertThat(item.getAvailableQuantity()).isEqualTo(10);
        assertThat(item.getReservedQuantity()).isZero();
    }

    @Test
    void refusesToReserveMoreThanIsAvailable() {
        InventoryItemEntity item = new InventoryItemEntity("SKU-1", 3);

        assertThat(item.canReserve(4)).isFalse();
        assertThatThrownBy(() -> item.reserve(4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only 3 available");
        assertThat(item.getAvailableQuantity()).isEqualTo(3);
    }

    @Test
    void treatsNonPositiveQuantitiesAsUnreservable() {
        InventoryItemEntity item = new InventoryItemEntity("SKU-1", 5);

        assertThat(item.canReserve(0)).isFalse();
        assertThat(item.canReserve(-1)).isFalse();
    }

    @Test
    void refusesNegativeStockLevels() {
        assertThatThrownBy(() -> new InventoryItemEntity("SKU-1", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InventoryItemEntity("SKU-1", 5).setAvailableQuantity(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
