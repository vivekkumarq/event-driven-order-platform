package com.vivek.platform.inventory.repository;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface InventoryItemRepository extends JpaRepository<InventoryItemEntity, UUID> {

    Optional<InventoryItemEntity> findBySku(String sku);

    boolean existsBySku(String sku);

    List<InventoryItemEntity> findAllByOrderBySkuAsc();
}
