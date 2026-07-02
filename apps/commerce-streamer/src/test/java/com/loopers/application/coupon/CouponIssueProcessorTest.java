package com.loopers.application.coupon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.infrastructure.coupon.IssuedCouponEntity;
import com.loopers.infrastructure.coupon.IssuedCouponJpaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.ZonedDateTime;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class CouponIssueProcessorTest {

    @Mock
    private IssuedCouponJpaRepository issuedCouponJpaRepository;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private CouponIssueProcessor processor;

    private static final Long COUPON_ID = 1L;
    private static final Long USER_ID = 100L;
    private static final String STOCK_KEY = CouponIssueProcessor.COUPON_STOCK_KEY_PREFIX + COUPON_ID;

    @BeforeEach
    void setUp() {
        processor = new CouponIssueProcessor(issuedCouponJpaRepository, new ObjectMapper(), redisTemplate);
    }

    private String buildPayload(Long couponId, Long userId) throws Exception {
        String expiredAt = ZonedDateTime.now().plusDays(30).toString();
        return new ObjectMapper().writeValueAsString(
            Map.of("couponId", couponId, "userId", userId, "expiredAt", expiredAt)
        );
    }

    @DisplayName("process()를 실행할 때,")
    @Nested
    class Process {

        @DisplayName("이미 발급된 쿠폰이면, 저장 없이 skip된다.")
        @Test
        void skipsIssuance_whenAlreadyIssued() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(true);

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should(never()).save(any());
            then(issuedCouponJpaRepository).should(never()).incrementIssuedCount(any());
        }

        @DisplayName("Redis 재고가 0이면, 발급 없이 skip된다.")
        @Test
        void skipsIssuance_whenStockExhausted() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(false);
            given(redisTemplate.hasKey(STOCK_KEY)).willReturn(true);
            given(redisTemplate.opsForValue()).willReturn(valueOperations);
            given(valueOperations.get(STOCK_KEY)).willReturn("0");

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should(never()).save(any());
            then(issuedCouponJpaRepository).should(never()).incrementIssuedCount(any());
            then(valueOperations).should(never()).decrement(anyString());
        }

        @DisplayName("Redis 재고 키가 없으면(무제한 쿠폰), 재고 체크 없이 발급된다.")
        @Test
        void issuesCoupon_whenNoStockKeyExists() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(false);
            given(redisTemplate.hasKey(STOCK_KEY)).willReturn(false);
            given(issuedCouponJpaRepository.save(any())).willReturn(mock(IssuedCouponEntity.class));

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should().save(any(IssuedCouponEntity.class));
            then(issuedCouponJpaRepository).should().incrementIssuedCount(COUPON_ID);
            then(redisTemplate).should(never()).opsForValue();
        }

        @DisplayName("재고가 충분하면, DB에 발급 저장 후 재고를 차감한다.")
        @Test
        void issuesCouponAndDecrementsStock_whenStockIsAvailable() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(false);
            given(redisTemplate.hasKey(STOCK_KEY)).willReturn(true);
            given(redisTemplate.opsForValue()).willReturn(valueOperations);
            given(valueOperations.get(STOCK_KEY)).willReturn("5");
            given(issuedCouponJpaRepository.save(any())).willReturn(mock(IssuedCouponEntity.class));

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should().save(any(IssuedCouponEntity.class));
            then(issuedCouponJpaRepository).should().incrementIssuedCount(COUPON_ID);
            then(valueOperations).should().decrement(STOCK_KEY);
        }
    }
}
