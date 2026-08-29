package com.vivek.platform.order.api;

import com.vivek.platform.order.api.dto.CreateOrderRequest;
import com.vivek.platform.order.api.dto.OrderResponse;
import com.vivek.platform.order.domain.OrderStatus;
import com.vivek.platform.order.service.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@Tag(name = "Orders", description = "Place, inspect and cancel orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    @Operation(summary = "Place an order",
            description = "Stores the order as PENDING and queues an OrderCreated event in the "
                    + "transactional outbox. The order becomes CONFIRMED or REJECTED once "
                    + "inventory-service answers with a reservation result.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Order accepted"),
            @ApiResponse(responseCode = "400", description = "Validation failed", content = @Content)
    })
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request,
                                                     UriComponentsBuilder uriBuilder) {
        OrderResponse response = OrderResponse.from(orderService.createOrder(request));
        URI location = uriBuilder.path("/api/orders/{id}").buildAndExpand(response.id()).toUri();
        return ResponseEntity.created(location).body(response);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Fetch an order by id")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Order found"),
            @ApiResponse(responseCode = "404", description = "No such order", content = @Content)
    })
    public OrderResponse getOrder(@PathVariable UUID id) {
        return OrderResponse.from(orderService.getOrder(id));
    }

    @GetMapping
    @Operation(summary = "List orders, newest first, optionally filtered by status")
    public List<OrderResponse> listOrders(
            @Parameter(description = "Restrict to a single lifecycle state")
            @RequestParam(required = false) OrderStatus status) {
        return orderService.listOrders(status).stream().map(OrderResponse::from).toList();
    }

    @PostMapping("/{id}/cancel")
    @Operation(summary = "Cancel an order",
            description = "Legal from PENDING and CONFIRMED. Cancelling a CONFIRMED order queues a "
                    + "compensating OrderCancelled event that releases the reserved stock.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Order cancelled"),
            @ApiResponse(responseCode = "404", description = "No such order", content = @Content),
            @ApiResponse(responseCode = "409", description = "Order is already terminal", content = @Content)
    })
    public OrderResponse cancelOrder(
            @PathVariable UUID id,
            @Parameter(description = "Free-text reason recorded on the order")
            @RequestParam(required = false) String reason) {
        return OrderResponse.from(orderService.cancelOrder(id, reason));
    }
}
