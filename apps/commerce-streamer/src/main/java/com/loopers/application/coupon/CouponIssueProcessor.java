package com.loopers.application.coupon;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.coupon.CouponIssueResult;
import com.loopers.infrastructure.coupon.CouponIssueEventEntity;
import com.loopers.infrastructure.coupon.CouponIssueEventJpaRepository;
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

/**
 * 쿠폰 발급 요청 이벤트를 처리한다. 중복 발급 방지 → 재고 체크 → 발급 → 재고 차감 순으로 진행하며,
 * 재고 수량은 Redis 카운터로 관리해 DB 락 없이 빠르게 소진 여부를 판단한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CouponIssueProcessor {

    static final String COUPON_STOCK_KEY_PREFIX = "coupon:stock:";

    private final IssuedCouponJpaRepository issuedCouponJpaRepository;
    private final CouponIssueEventJpaRepository couponIssueEventJpaRepository;
    private final ObjectMapper objectMapper;

    @Qualifier("redisTemplateMaster")
    private final RedisTemplate<String, String> redisTemplate;

    @Transactional
    public void process(String payload) throws Exception {
        Map<String, Object> data = objectMapper.readValue(payload, new TypeReference<>() {});
        String eventId = (String) data.get("eventId");
        Long couponId = ((Number) data.get("couponId")).longValue();
        Long userId = ((Number) data.get("userId")).longValue();
        ZonedDateTime expiredAt = ZonedDateTime.parse((String) data.get("expiredAt"));

        // 1. 중복 발급 체크
        if (issuedCouponJpaRepository.existsByCouponIdAndUserId(couponId, userId)) {
            log.debug("[COUPON_ISSUE] 중복 발급 skip — couponId={}, userId={}", couponId, userId);
            saveOrUpdateEvent(eventId, couponId, userId, CouponIssueResult.DUPLICATE);
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
                saveOrUpdateEvent(eventId, couponId, userId, CouponIssueResult.OUT_OF_STOCK);
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

        saveOrUpdateEvent(eventId, couponId, userId, CouponIssueResult.SUCCESS);
        log.info("[COUPON_ISSUE] 발급 완료 — couponId={}, userId={}", couponId, userId);
    }

    private void saveOrUpdateEvent(String eventId, Long couponId, Long userId, CouponIssueResult result) {
        couponIssueEventJpaRepository.findByEventId(eventId).ifPresentOrElse(
            entity -> {
                entity.updateResult(result);
                couponIssueEventJpaRepository.save(entity);
            },
            () -> couponIssueEventJpaRepository.save(new CouponIssueEventEntity(eventId, couponId, userId, result))
        );
    }
}
