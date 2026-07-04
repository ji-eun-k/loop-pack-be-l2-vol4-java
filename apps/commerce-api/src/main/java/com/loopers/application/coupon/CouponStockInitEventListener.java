package com.loopers.application.coupon;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponStockInitEventListener {

    static final String COUPON_STOCK_KEY_PREFIX = "coupon:stock:";

    @Qualifier("redisTemplateMaster")
    private final RedisTemplate<String, String> redisTemplate;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(CouponStockInitEvent event) {
        String key = COUPON_STOCK_KEY_PREFIX + event.couponId();
        redisTemplate.opsForValue().set(key, String.valueOf(event.maxIssuanceCount()));
        log.info("[COUPON] Redis 재고 초기화 — couponId={}, stock={}", event.couponId(), event.maxIssuanceCount());
    }
}
