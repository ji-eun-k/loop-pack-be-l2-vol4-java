package com.loopers.application.payment;

public record PaymentCompletedEvent(Long orderId, Long userId) {
}