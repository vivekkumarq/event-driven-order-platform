package com.vivek.platform.inventory.service;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import com.vivek.platform.inventory.exception.DuplicateSkuException;
import com.vivek.platform.inventory.exception.InventoryItemNotFoundException;
import com.vivek.platform.inventory.repository.InventoryItemRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Stock administration: the REST-facing half of the inventory domain. */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryItemRepository repository;

    public InventoryService(InventoryItemRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public InventoryItemEntity createItem(String sku, int availableQuantity) {
        if (repository.existsBySku(sku)) {
            throw new DuplicateSkuException(sku);
        }
        InventoryItemEntity saved = repository.save(new InventoryItemEntity(sku, availableQuantity));
        log.info("Created inventory item {} with {} available units", sku, availableQuantity);
        return saved;
    }

    @Transactional(readOnly = true)
    public InventoryItemEntity getBySku(String sku) {
        return repository.findBySku(sku).orElseThrow(() -> new InventoryItemNotFoundException(sku));
    }

    @Transactional(readOnly = true)
    public List<InventoryItemEntity> listItems() {
        return repository.findAllByOrderBySkuAsc();
    }

    /** Replaces the sellable stock level. Reserved units are not touched. */
    @Transactional
    public InventoryItemEntity updateAvailableQuantity(String sku, int availableQuantity) {
        InventoryItemEntity item = getBySku(sku);
        item.setAvailableQuantity(availableQuantity);
        InventoryItemEntity saved = repository.save(item);
        log.info("Set available stock for {} to {}", sku, availableQuantity);
        return saved;
    }

    @Transactional
    public void deleteItem(String sku) {
        InventoryItemEntity item = getBySku(sku);
        repository.delete(item);
        log.info("Deleted inventory item {}", sku);
    }
}
