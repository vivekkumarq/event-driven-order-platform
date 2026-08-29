package com.vivek.platform.order.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStatusTest {

    @ParameterizedTest(name = "{0} -> {1} is legal")
    @CsvSource({
            "PENDING, CONFIRMED",
            "PENDING, REJECTED",
            "PENDING, CANCELLED",
            "CONFIRMED, CANCELLED"
    })
    void allowsLegalTransitions(OrderStatus from, OrderStatus to) {
        assertThat(from.canTransitionTo(to)).isTrue();
    }

    @ParameterizedTest(name = "{0} -> {1} is rejected")
    @CsvSource({
            "PENDING, PENDING",
            "CONFIRMED, PENDING",
            "CONFIRMED, CONFIRMED",
            "CONFIRMED, REJECTED",
            "REJECTED, CONFIRMED",
            "REJECTED, CANCELLED",
            "CANCELLED, CONFIRMED",
            "CANCELLED, REJECTED",
            "CANCELLED, CANCELLED"
    })
    void rejectsIllegalTransitions(OrderStatus from, OrderStatus to) {
        assertThat(from.canTransitionTo(to)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(OrderStatus.class)
    void neverAllowsANullTarget(OrderStatus from) {
        assertThat(from.canTransitionTo(null)).isFalse();
    }

    @Test
    void onlyRejectedAndCancelledAreTerminal() {
        assertThat(OrderStatus.PENDING.isTerminal()).isFalse();
        assertThat(OrderStatus.CONFIRMED.isTerminal()).isFalse();
        assertThat(OrderStatus.REJECTED.isTerminal()).isTrue();
        assertThat(OrderStatus.CANCELLED.isTerminal()).isTrue();
    }

    @Test
    @DisplayName("the entity enforces the transition rules, not just the enum")
    void entityRefusesAnIllegalTransition() {
        OrderEntity order = new OrderEntity("SKU-1", 1, new BigDecimal("10.00"));
        order.transitionTo(OrderStatus.CONFIRMED, null);

        assertThatThrownBy(() -> order.transitionTo(OrderStatus.REJECTED, "too late"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CONFIRMED -> REJECTED");
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void newOrdersStartPending() {
        OrderEntity order = new OrderEntity("SKU-1", 3, new BigDecimal("99.95"));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getAmount()).isEqualByComparingTo("99.95");
        assertThat(order.getCreatedAt()).isNotNull();
    }
}
