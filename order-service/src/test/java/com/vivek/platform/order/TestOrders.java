package com.vivek.platform.order;

import com.vivek.platform.order.domain.OrderEntity;
import com.vivek.platform.order.domain.OrderStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Builders for order fixtures.
 *
 * <p>Ids are normally assigned by Hibernate, so unit tests that never touch a database set them
 * reflectively rather than adding a setter to production code purely for tests.
 */
public final class TestOrders {

    private TestOrders() {
    }

    public static OrderEntity pending(UUID id, String sku, int quantity, String amount) {
        OrderEntity order = new OrderEntity(sku, quantity, new BigDecimal(amount));
        ReflectionTestUtils.setField(order, "id", id);
        return order;
    }

    public static OrderEntity pending(UUID id) {
        return pending(id, "SKU-1", 2, "199.99");
    }

    public static OrderEntity inStatus(UUID id, OrderStatus status) {
        OrderEntity order = pending(id);
        if (status != OrderStatus.PENDING) {
            order.transitionTo(status, null);
        }
        return order;
    }
}
