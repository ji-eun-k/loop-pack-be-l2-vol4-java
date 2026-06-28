package com.loopers.application.like;

import com.loopers.application.product.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@RequiredArgsConstructor
@Component
public class LikeCountEventListener {

    private final ProductService productService;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(LikeCountChangedEvent event) {
        try {
            if (event.increase()) {
                productService.incrementLikeCount(event.productId());
            } else {
                productService.decrementLikeCount(event.productId());
            }
        } catch (Exception e) {
            log.warn("좋아요 집계 업데이트 실패 — productId={}, increase={}", event.productId(), event.increase(), e);
        }
    }
}