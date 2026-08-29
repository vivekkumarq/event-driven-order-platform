package com.vivek.platform.inventory.api;

import com.vivek.platform.inventory.domain.InventoryItemEntity;
import com.vivek.platform.inventory.exception.DuplicateSkuException;
import com.vivek.platform.inventory.exception.InventoryItemNotFoundException;
import com.vivek.platform.inventory.service.InventoryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(InventoryController.class)
@ActiveProfiles("test")
class InventoryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private InventoryService inventoryService;

    private InventoryItemEntity item(String sku, int available) {
        return new InventoryItemEntity(sku, available);
    }

    @Test
    void createsAnItem() throws Exception {
        when(inventoryService.createItem("SKU-1", 100)).thenReturn(item("SKU-1", 100));

        mockMvc.perform(post("/api/inventory")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"SKU-1","availableQuantity":100}"""))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.sku").value("SKU-1"))
                .andExpect(jsonPath("$.availableQuantity").value(100))
                .andExpect(jsonPath("$.reservedQuantity").value(0))
                .andExpect(jsonPath("$.totalQuantity").value(100));
    }

    @Test
    void rejectsANegativeOpeningStockLevel() throws Exception {
        mockMvc.perform(post("/api/inventory")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"SKU-1","availableQuantity":-5}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.availableQuantity").exists());
    }

    @Test
    void rejectsABlankSku() throws Exception {
        mockMvc.perform(post("/api/inventory")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"","availableQuantity":5}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.sku").exists());
    }

    @Test
    void returns409ForADuplicateSku() throws Exception {
        when(inventoryService.createItem(anyString(), anyInt()))
                .thenThrow(new DuplicateSkuException("SKU-1"));

        mockMvc.perform(post("/api/inventory")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"SKU-1","availableQuantity":5}"""))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title").value("Duplicate SKU"));
    }

    @Test
    void listsItems() throws Exception {
        when(inventoryService.listItems()).thenReturn(List.of(item("SKU-1", 5), item("SKU-2", 7)));

        mockMvc.perform(get("/api/inventory"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].sku").value("SKU-1"));
    }

    @Test
    void returnsOneItem() throws Exception {
        InventoryItemEntity entity = item("SKU-1", 10);
        entity.reserve(4);
        when(inventoryService.getBySku("SKU-1")).thenReturn(entity);

        mockMvc.perform(get("/api/inventory/{sku}", "SKU-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.availableQuantity").value(6))
                .andExpect(jsonPath("$.reservedQuantity").value(4))
                .andExpect(jsonPath("$.totalQuantity").value(10));
    }

    @Test
    void returns404ForAnUnknownSku() throws Exception {
        when(inventoryService.getBySku("SKU-NOPE"))
                .thenThrow(new InventoryItemNotFoundException("SKU-NOPE"));

        mockMvc.perform(get("/api/inventory/{sku}", "SKU-NOPE"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Inventory item not found"))
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void updatesTheStockLevel() throws Exception {
        when(inventoryService.updateAvailableQuantity("SKU-1", 250)).thenReturn(item("SKU-1", 250));

        mockMvc.perform(put("/api/inventory/{sku}", "SKU-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"availableQuantity":250}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.availableQuantity").value(250));
    }

    @Test
    void deletesAnItem() throws Exception {
        mockMvc.perform(delete("/api/inventory/{sku}", "SKU-1"))
                .andExpect(status().isNoContent());

        verify(inventoryService).deleteItem("SKU-1");
    }

    @Test
    void returns404WhenDeletingAnUnknownSku() throws Exception {
        doThrow(new InventoryItemNotFoundException("SKU-NOPE"))
                .when(inventoryService).deleteItem("SKU-NOPE");

        mockMvc.perform(delete("/api/inventory/{sku}", "SKU-NOPE"))
                .andExpect(status().isNotFound());
    }
}
