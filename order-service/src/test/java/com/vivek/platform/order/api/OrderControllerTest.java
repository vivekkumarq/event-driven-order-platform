package com.vivek.platform.order.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.platform.order.TestOrders;
import com.vivek.platform.order.api.dto.CreateOrderRequest;
import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import com.vivek.platform.order.exception.IllegalOrderTransitionException;
import com.vivek.platform.order.exception.OrderNotFoundException;
import com.vivek.platform.order.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
@ActiveProfiles("test")
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private OrderService orderService;

    @Test
    void createsAnOrderAndReturns201WithALocationHeader() throws Exception {
        UUID id = UUID.randomUUID();
        when(orderService.createOrder(any(CreateOrderRequest.class)))
                .thenReturn(TestOrders.pending(id, "SKU-LAPTOP-01", 2, "1499.99"));

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new CreateOrderRequest(
                                "SKU-LAPTOP-01", 2, new BigDecimal("1499.99")))))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "http://localhost/api/orders/" + id))
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.sku").value("SKU-LAPTOP-01"))
                .andExpect(jsonPath("$.quantity").value(2))
                .andExpect(jsonPath("$.amount").value(1499.99))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void rejectsAnOrderWithANegativeAmount() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"SKU-1","quantity":1,"amount":-5.00}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Invalid request"))
                .andExpect(jsonPath("$.errors.amount").exists());
    }

    @Test
    void rejectsAnOrderWithAZeroQuantityAndABlankSku() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"  ","quantity":0,"amount":10.00}"""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.sku").exists())
                .andExpect(jsonPath("$.errors.quantity").exists());
    }

    @Test
    void rejectsAnOrderWithMissingFields() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returnsAnOrderById() throws Exception {
        UUID id = UUID.randomUUID();
        when(orderService.getOrder(id)).thenReturn(TestOrders.pending(id));

        mockMvc.perform(get("/api/orders/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()));
    }

    @Test
    void returns404ForAnUnknownOrder() throws Exception {
        UUID id = UUID.randomUUID();
        when(orderService.getOrder(id)).thenThrow(new OrderNotFoundException(id));

        mockMvc.perform(get("/api/orders/{id}", id))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Order not found"))
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void listsOrdersFilteredByStatus() throws Exception {
        OrderEntity order = TestOrders.inStatus(UUID.randomUUID(), OrderStatus.CONFIRMED);
        when(orderService.listOrders(OrderStatus.CONFIRMED)).thenReturn(List.of(order));

        mockMvc.perform(get("/api/orders").param("status", "CONFIRMED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].status").value("CONFIRMED"));
    }

    @Test
    void cancelsAnOrder() throws Exception {
        UUID id = UUID.randomUUID();
        OrderEntity cancelled = TestOrders.inStatus(id, OrderStatus.CANCELLED);
        when(orderService.cancelOrder(eq(id), any())).thenReturn(cancelled);

        mockMvc.perform(post("/api/orders/{id}/cancel", id).param("reason", "changed my mind"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void returns409WhenCancellingATerminalOrder() throws Exception {
        UUID id = UUID.randomUUID();
        when(orderService.cancelOrder(eq(id), any()))
                .thenThrow(new IllegalOrderTransitionException(OrderStatus.REJECTED, OrderStatus.CANCELLED));

        mockMvc.perform(post("/api/orders/{id}/cancel", id))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title").value("Illegal order state transition"));
    }
}
