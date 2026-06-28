package com.loopers.application.order;

import com.loopers.domain.order.Order;

public record OrderCreatedEvent(Order order) {
}