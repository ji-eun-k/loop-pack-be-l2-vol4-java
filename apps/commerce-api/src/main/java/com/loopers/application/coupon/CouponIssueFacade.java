package com.loopers.application.coupon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.coupon.Coupon;
import com.loopers.domain.coupon.CouponIssueResult;
import com.loopers.infrastructure.coupon.CouponIssueEventEntity;
import com.loopers.infrastructure.coupon.CouponIssueEventJpaRepository;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Component
public class CouponIssueFacade {

    static final String TOPIC = "coupon-issue-requests";

    private final CouponService couponService;
    private final CouponIssueEventJpaRepository couponIssueEventJpaRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Transactional
    public String requestIssue(Long couponId, Long userId) {
        Coupon coupon = couponService.getCoupon(couponId);

        if (coupon.getExpiredAt().isBefore(ZonedDateTime.now())) {
            throw new CoreException(ErrorType.BAD_REQUEST, "만료된 쿠폰입니다.");
        }

        String eventId = UUID.randomUUID().toString();
        couponIssueEventJpaRepository.save(
            new CouponIssueEventEntity(eventId, couponId, userId, CouponIssueResult.PENDING)
        );

        try {
            String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", eventId,
                "couponId", couponId,
                "userId", userId,
                "expiredAt", coupon.getExpiredAt().toString()
            ));
            ProducerRecord<Object, Object> record = new ProducerRecord<>(TOPIC, null, couponId.toString(), payload);
            record.headers().add("X-Event-Type", "CouponIssueRequested".getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
            log.debug("[COUPON] 발급 요청 발행 — eventId={}, couponId={}, userId={}", eventId, couponId, userId);
        } catch (Exception e) {
            throw new CoreException(ErrorType.INTERNAL_ERROR, "쿠폰 발급 요청에 실패했습니다.");
        }
        return eventId;
    }
}
