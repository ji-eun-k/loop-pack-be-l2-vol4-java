package com.loopers.application.coupon;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.infrastructure.coupon.IssuedCouponEntity;
import com.loopers.infrastructure.coupon.IssuedCouponJpaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.ZonedDateTime;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponIssueProcessor {

    static final String COUPON_STOCK_KEY_PREFIX = "coupon:stock:";

    private final IssuedCouponJpaRepository issuedCouponJpaRepository;
    private final ObjectMapper objectMapper;

    @Qualifier("redisTemplateMaster")
    private final RedisTemplate<String, String> redisTemplate;

    @Transactional
    public void process(String payload) throws Exception {
        Map<String, Object> data = objectMapper.readValue(payload, new TypeReference<>() {});
        Long couponId = ((Number) data.get("couponId")).longValue();
        Long userId = ((Number) data.get("userId")).longValue();
        ZonedDateTime expiredAt = ZonedDateTime.parse((String) data.get("expiredAt"));

        // 1. 중복 발급 체크
        if (issuedCouponJpaRepository.existsByCouponIdAndUserId(couponId, userId)) {
            log.debug("[COUPON_ISSUE] 중복 발급 skip — couponId={}, userId={}", couponId, userId);
            return;
        }

        // 2. 재고 체크 (key 없으면 무제한)
        String stockKey = COUPON_STOCK_KEY_PREFIX + couponId;
        boolean isLimited = Boolean.TRUE.equals(redisTemplate.hasKey(stockKey));
        if (isLimited) {
            String stockValue = redisTemplate.opsForValue().get(stockKey);
            long stock = stockValue != null ? Long.parseLong(stockValue) : 0L;
            if (stock <= 0) {
                log.debug("[COUPON_ISSUE] 재고 소진 — couponId={}, userId={}", couponId, userId);
                return;
            }
        }

        // 3. 발급 저장 + issuedCount 증가
        issuedCouponJpaRepository.save(new IssuedCouponEntity(couponId, userId, expiredAt));
        issuedCouponJpaRepository.incrementIssuedCount(couponId);

        // 4. 재고 차감
        if (isLimited) {
            redisTemplate.opsForValue().decrement(stockKey);
        }

        log.info("[COUPON_ISSUE] 발급 완료 — couponId={}, userId={}", couponId, userId);
    }
}
