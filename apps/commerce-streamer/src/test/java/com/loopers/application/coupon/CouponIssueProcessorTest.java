package com.loopers.application.coupon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.coupon.CouponIssueResult;
import com.loopers.infrastructure.coupon.CouponIssueEventEntity;
import com.loopers.infrastructure.coupon.CouponIssueEventJpaRepository;
import com.loopers.infrastructure.coupon.IssuedCouponEntity;
import com.loopers.infrastructure.coupon.IssuedCouponJpaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
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
    private CouponIssueEventJpaRepository couponIssueEventJpaRepository;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private CouponIssueProcessor processor;

    private static final Long COUPON_ID = 1L;
    private static final Long USER_ID = 100L;
    private static final String EVENT_ID = UUID.randomUUID().toString();
    private static final String STOCK_KEY = CouponIssueProcessor.COUPON_STOCK_KEY_PREFIX + COUPON_ID;

    @BeforeEach
    void setUp() {
        processor = new CouponIssueProcessor(issuedCouponJpaRepository, couponIssueEventJpaRepository, new ObjectMapper(), redisTemplate);
    }

    private String buildPayload(Long couponId, Long userId) throws Exception {
        String expiredAt = ZonedDateTime.now().plusDays(30).toString();
        return new ObjectMapper().writeValueAsString(
            Map.of("eventId", EVENT_ID, "couponId", couponId, "userId", userId, "expiredAt", expiredAt)
        );
    }

    @DisplayName("process()를 실행할 때,")
    @Nested
    class Process {

        @DisplayName("이미 발급된 쿠폰이고 PENDING 레코드 없으면, DUPLICATE 이벤트를 INSERT한다.")
        @Test
        void insertsDuplicateEvent_whenAlreadyIssuedAndNoPendingRecord() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(true);
            given(couponIssueEventJpaRepository.findByEventId(EVENT_ID)).willReturn(Optional.empty());

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should(never()).save(any());
            then(issuedCouponJpaRepository).should(never()).incrementIssuedCount(any());
            ArgumentCaptor<CouponIssueEventEntity> captor = ArgumentCaptor.forClass(CouponIssueEventEntity.class);
            then(couponIssueEventJpaRepository).should().save(captor.capture());
            assertThat(captor.getValue().getResult()).isEqualTo(CouponIssueResult.DUPLICATE);
        }

        @DisplayName("이미 발급된 쿠폰이고 PENDING 레코드 있으면, DUPLICATE로 UPDATE한다.")
        @Test
        void updatesToDuplicateEvent_whenAlreadyIssuedAndPendingRecordExists() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            CouponIssueEventEntity pendingEntity = new CouponIssueEventEntity(EVENT_ID, COUPON_ID, USER_ID, CouponIssueResult.PENDING);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(true);
            given(couponIssueEventJpaRepository.findByEventId(EVENT_ID)).willReturn(Optional.of(pendingEntity));

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should(never()).save(any());
            ArgumentCaptor<CouponIssueEventEntity> captor = ArgumentCaptor.forClass(CouponIssueEventEntity.class);
            then(couponIssueEventJpaRepository).should().save(captor.capture());
            assertThat(captor.getValue()).isSameAs(pendingEntity);
            assertThat(captor.getValue().getResult()).isEqualTo(CouponIssueResult.DUPLICATE);
        }

        @DisplayName("Redis 재고가 0이고 PENDING 레코드 있으면, OUT_OF_STOCK으로 UPDATE한다.")
        @Test
        void updatesToOutOfStockEvent_whenStockExhaustedAndPendingRecordExists() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            CouponIssueEventEntity pendingEntity = new CouponIssueEventEntity(EVENT_ID, COUPON_ID, USER_ID, CouponIssueResult.PENDING);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(false);
            given(redisTemplate.hasKey(STOCK_KEY)).willReturn(true);
            given(redisTemplate.opsForValue()).willReturn(valueOperations);
            given(valueOperations.get(STOCK_KEY)).willReturn("0");
            given(couponIssueEventJpaRepository.findByEventId(EVENT_ID)).willReturn(Optional.of(pendingEntity));

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should(never()).save(any());
            then(valueOperations).should(never()).decrement(anyString());
            ArgumentCaptor<CouponIssueEventEntity> captor = ArgumentCaptor.forClass(CouponIssueEventEntity.class);
            then(couponIssueEventJpaRepository).should().save(captor.capture());
            assertThat(captor.getValue()).isSameAs(pendingEntity);
            assertThat(captor.getValue().getResult()).isEqualTo(CouponIssueResult.OUT_OF_STOCK);
        }

        @DisplayName("Redis 재고 키가 없으면(무제한 쿠폰), PENDING 레코드를 SUCCESS로 UPDATE한다.")
        @Test
        void updatesToSuccessEvent_whenNoStockKeyAndPendingRecordExists() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            CouponIssueEventEntity pendingEntity = new CouponIssueEventEntity(EVENT_ID, COUPON_ID, USER_ID, CouponIssueResult.PENDING);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(false);
            given(redisTemplate.hasKey(STOCK_KEY)).willReturn(false);
            given(issuedCouponJpaRepository.save(any())).willReturn(mock(IssuedCouponEntity.class));
            given(couponIssueEventJpaRepository.findByEventId(EVENT_ID)).willReturn(Optional.of(pendingEntity));

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should().save(any(IssuedCouponEntity.class));
            then(issuedCouponJpaRepository).should().incrementIssuedCount(COUPON_ID);
            then(redisTemplate).should(never()).opsForValue();
            ArgumentCaptor<CouponIssueEventEntity> captor = ArgumentCaptor.forClass(CouponIssueEventEntity.class);
            then(couponIssueEventJpaRepository).should().save(captor.capture());
            assertThat(captor.getValue()).isSameAs(pendingEntity);
            assertThat(captor.getValue().getResult()).isEqualTo(CouponIssueResult.SUCCESS);
        }

        @DisplayName("재고가 충분하면, PENDING 레코드를 SUCCESS로 UPDATE하고 재고를 차감한다.")
        @Test
        void updatesToSuccessEventAndDecrementsStock_whenStockIsAvailable() throws Exception {
            // Arrange
            String payload = buildPayload(COUPON_ID, USER_ID);
            CouponIssueEventEntity pendingEntity = new CouponIssueEventEntity(EVENT_ID, COUPON_ID, USER_ID, CouponIssueResult.PENDING);
            given(issuedCouponJpaRepository.existsByCouponIdAndUserId(COUPON_ID, USER_ID)).willReturn(false);
            given(redisTemplate.hasKey(STOCK_KEY)).willReturn(true);
            given(redisTemplate.opsForValue()).willReturn(valueOperations);
            given(valueOperations.get(STOCK_KEY)).willReturn("5");
            given(issuedCouponJpaRepository.save(any())).willReturn(mock(IssuedCouponEntity.class));
            given(couponIssueEventJpaRepository.findByEventId(EVENT_ID)).willReturn(Optional.of(pendingEntity));

            // Act
            processor.process(payload);

            // Assert
            then(issuedCouponJpaRepository).should().save(any(IssuedCouponEntity.class));
            then(issuedCouponJpaRepository).should().incrementIssuedCount(COUPON_ID);
            then(valueOperations).should().decrement(STOCK_KEY);
            ArgumentCaptor<CouponIssueEventEntity> captor = ArgumentCaptor.forClass(CouponIssueEventEntity.class);
            then(couponIssueEventJpaRepository).should().save(captor.capture());
            assertThat(captor.getValue()).isSameAs(pendingEntity);
            assertThat(captor.getValue().getResult()).isEqualTo(CouponIssueResult.SUCCESS);
        }
    }
}
