package com.vivek.platform.order.api;

import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.service.OrderService;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public OrderEntity createOrder(@RequestParam Double amount) {
        return orderService.createOrder(amount);
    }

    @GetMapping("/{id}")
    public OrderEntity getOrder(@PathVariable UUID id) {
        return orderService.getOrder(id);
    }
}
