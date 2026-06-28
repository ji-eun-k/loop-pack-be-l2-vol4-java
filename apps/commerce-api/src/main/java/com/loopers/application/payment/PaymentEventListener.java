package com.loopers.application.payment;

import com.loopers.application.order.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@RequiredArgsConstructor
@Component
public class PaymentEventListener {

    private final OrderService orderService;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(PaymentCompletedEvent event) {
        try {
            orderService.confirm(event.orderId());
        } catch (Exception e) {
            log.error("주문 확정 실패 — orderId={}, userId={}", event.orderId(), event.userId(), e);
        }
    }
}