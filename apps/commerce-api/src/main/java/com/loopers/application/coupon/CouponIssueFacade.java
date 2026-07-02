package com.loopers.application.coupon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.coupon.Coupon;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Component
public class CouponIssueFacade {

    static final String TOPIC = "coupon-issue-requests";

    private final CouponService couponService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void requestIssue(Long couponId, Long userId) {
        Coupon coupon = couponService.getCoupon(couponId);

        if (coupon.getExpiredAt().isBefore(ZonedDateTime.now())) {
            throw new CoreException(ErrorType.BAD_REQUEST, "만료된 쿠폰입니다.");
        }

        try {
            String payload = objectMapper.writeValueAsString(Map.of(
                "couponId", couponId,
                "userId", userId,
                "expiredAt", coupon.getExpiredAt().toString()
            ));
            ProducerRecord<Object, Object> record = new ProducerRecord<>(TOPIC, null, couponId.toString(), payload);
            record.headers().add("X-Event-Type", "CouponIssueRequested".getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
            log.debug("[COUPON] 발급 요청 발행 — couponId={}, userId={}", couponId, userId);
        } catch (Exception e) {
            throw new CoreException(ErrorType.INTERNAL_ERROR, "쿠폰 발급 요청에 실패했습니다.");
        }
    }
}
