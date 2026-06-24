package com.loopers.application.payment;

import com.loopers.application.order.OrderService;
import com.loopers.domain.payment.Payment;
import com.loopers.domain.payment.PaymentStatus;
import com.loopers.infrastructure.pg.PgApiResponse;
import com.loopers.infrastructure.pg.PgFeignClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class PaymentPollingScheduler {

    private static final int MAX_POLLING_COUNT = 5;

    private final PaymentService paymentService;
    private final PgFeignClient pgFeignClient;
    private final OrderService orderService;

    @Scheduled(fixedDelay = 60_000)
    public void pollPendingPayments() {
        List<Payment> payments = paymentService.findAllPendingOrInProgress();
        for (Payment payment : payments) {
            try {
                processPayment(payment);
            } catch (Exception e) {
                log.warn("결제 폴링 처리 실패: paymentId={}", payment.getId(), e);
            }
        }
    }

    private void processPayment(Payment payment) {
        if (payment.getStatus() == PaymentStatus.CREATED) {
            Payment recorded = paymentService.recordPolling(payment);
            if (recorded.getPollingCount() >= MAX_POLLING_COUNT) {
                paymentService.abandon(recorded);
                log.info("결제 포기 처리: paymentId={}, pollingCount={}", recorded.getId(), recorded.getPollingCount());
            }
            return;
        }

        try {
            PgApiResponse.PaymentStatus pgStatus = pgFeignClient.getPaymentStatus(
                String.valueOf(payment.getUserId()),
                payment.getTransactionKey()
            );

            Payment recorded = paymentService.recordPolling(payment);

            if (recorded.getPollingCount() >= MAX_POLLING_COUNT) {
                paymentService.abandon(recorded);
                log.info("결제 포기 처리: paymentId={}, pollingCount={}", recorded.getId(), recorded.getPollingCount());
                return;
            }

            PaymentStatus finalStatus = PaymentStatus.valueOf(pgStatus.status());
            if (finalStatus == PaymentStatus.SUCCESS || finalStatus == PaymentStatus.FAILED) {
                paymentService.complete(recorded.getTransactionKey(), finalStatus, pgStatus.reason());
                if (finalStatus == PaymentStatus.SUCCESS) {
                    orderService.confirm(recorded.getOrderId());
                }
            }
        } catch (Exception e) {
            log.warn("PG 상태 조회 실패: transactionKey={}", payment.getTransactionKey(), e);
        }
    }
}