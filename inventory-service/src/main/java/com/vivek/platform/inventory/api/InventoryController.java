package com.vivek.platform.inventory.api;

import com.vivek.platform.inventory.api.dto.CreateInventoryItemRequest;
import com.vivek.platform.inventory.api.dto.InventoryItemResponse;
import com.vivek.platform.inventory.api.dto.UpdateStockRequest;
import com.vivek.platform.inventory.service.InventoryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/inventory")
@Tag(name = "Inventory", description = "Stock levels per SKU")
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @PostMapping
    @Operation(summary = "Create a SKU with an opening stock level")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Item created"),
            @ApiResponse(responseCode = "400", description = "Validation failed", content = @Content),
            @ApiResponse(responseCode = "409", description = "SKU already exists", content = @Content)
    })
    public ResponseEntity<InventoryItemResponse> createItem(
            @Valid @RequestBody CreateInventoryItemRequest request, UriComponentsBuilder uriBuilder) {
        InventoryItemResponse response = InventoryItemResponse.from(
                inventoryService.createItem(request.sku(), request.availableQuantity()));
        URI location = uriBuilder.path("/api/inventory/{sku}").buildAndExpand(response.sku()).toUri();
        return ResponseEntity.created(location).body(response);
    }

    @GetMapping
    @Operation(summary = "List every SKU, ordered by SKU")
    public List<InventoryItemResponse> listItems() {
        return inventoryService.listItems().stream().map(InventoryItemResponse::from).toList();
    }

    @GetMapping("/{sku}")
    @Operation(summary = "Fetch the stock position for one SKU")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Item found"),
            @ApiResponse(responseCode = "404", description = "No such SKU", content = @Content)
    })
    public InventoryItemResponse getItem(@PathVariable String sku) {
        return InventoryItemResponse.from(inventoryService.getBySku(sku));
    }

    @PutMapping("/{sku}")
    @Operation(summary = "Replace the sellable stock level",
            description = "Sets availableQuantity. Units already reserved for orders are untouched.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Stock updated"),
            @ApiResponse(responseCode = "400", description = "Validation failed", content = @Content),
            @ApiResponse(responseCode = "404", description = "No such SKU", content = @Content)
    })
    public InventoryItemResponse updateStock(@PathVariable String sku,
                                             @Valid @RequestBody UpdateStockRequest request) {
        return InventoryItemResponse.from(
                inventoryService.updateAvailableQuantity(sku, request.availableQuantity()));
    }

    @DeleteMapping("/{sku}")
    @Operation(summary = "Delete a SKU")
    @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Item deleted"),
            @ApiResponse(responseCode = "404", description = "No such SKU", content = @Content)
    })
    public ResponseEntity<Void> deleteItem(@PathVariable String sku) {
        inventoryService.deleteItem(sku);
        return ResponseEntity.noContent().build();
    }
}
