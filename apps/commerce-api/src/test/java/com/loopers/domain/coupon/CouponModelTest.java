package com.loopers.domain.coupon;

import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CouponModelTest {

    private static final ZonedDateTime FUTURE = ZonedDateTime.now().plusDays(30);

    @DisplayName("쿠폰을 생성할 때,")
    @Nested
    class Create {

        @DisplayName("유효한 FIXED 타입 정보로 생성하면, 정상적으로 생성된다.")
        @Test
        void createsCoupon_withFixedType_whenValidInfoIsProvided() {
            Coupon coupon = new Coupon("신규가입 1000원 할인", CouponType.FIXED, BigDecimal.valueOf(1000), BigDecimal.valueOf(5000), FUTURE, 0);

            assertAll(
                () -> assertThat(coupon.getName()).isEqualTo("신규가입 1000원 할인"),
                () -> assertThat(coupon.getType()).isEqualTo(CouponType.FIXED),
                () -> assertThat(coupon.getValue()).isEqualByComparingTo(BigDecimal.valueOf(1000)),
                () -> assertThat(coupon.getMinOrderAmount()).isEqualByComparingTo(BigDecimal.valueOf(5000)),
                () -> assertThat(coupon.getExpiredAt()).isEqualTo(FUTURE)
            );
        }

        @DisplayName("유효한 RATE 타입 정보로 생성하면, 정상적으로 생성된다.")
        @Test
        void createsCoupon_withRateType_whenValidInfoIsProvided() {
            Coupon coupon = new Coupon("신규가입 10% 할인", CouponType.RATE, BigDecimal.valueOf(10), null, FUTURE, 0);

            assertAll(
                () -> assertThat(coupon.getType()).isEqualTo(CouponType.RATE),
                () -> assertThat(coupon.getValue()).isEqualByComparingTo(BigDecimal.valueOf(10)),
                () -> assertThat(coupon.getMinOrderAmount()).isNull()
            );
        }

        @DisplayName("name이 null이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenNameIsNull() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon(null, CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("name이 빈 문자열이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenNameIsBlank() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("  ", CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("type이 null이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenTypeIsNull() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("테스트 쿠폰", null, BigDecimal.valueOf(1000), null, FUTURE, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("FIXED 타입에서 value가 0이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenFixedValueIsZero() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("테스트 쿠폰", CouponType.FIXED, BigDecimal.ZERO, null, FUTURE, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("FIXED 타입에서 value가 음수이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenFixedValueIsNegative() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("테스트 쿠폰", CouponType.FIXED, BigDecimal.valueOf(-1), null, FUTURE, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("RATE 타입에서 value가 0이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenRateValueIsZero() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("테스트 쿠폰", CouponType.RATE, BigDecimal.ZERO, null, FUTURE, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("RATE 타입에서 value가 100 초과이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenRateValueExceedsHundred() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("테스트 쿠폰", CouponType.RATE, BigDecimal.valueOf(101), null, FUTURE, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("RATE 타입에서 value가 100이면, 정상적으로 생성된다.")
        @Test
        void createsCoupon_whenRateValueIsHundred() {
            Coupon coupon = new Coupon("100% 할인 쿠폰", CouponType.RATE, BigDecimal.valueOf(100), null, FUTURE, 0);
            assertThat(coupon.getValue()).isEqualByComparingTo(BigDecimal.valueOf(100));
        }

        @DisplayName("expiredAt이 null이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenExpiredAtIsNull() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("테스트 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), null, null, 0));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("maxIssuanceCount가 음수이면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenMaxIssuanceCountIsNegative() {
            CoreException ex = assertThrows(CoreException.class,
                () -> new Coupon("테스트 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, -1));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("maxIssuanceCount가 0이면, 무제한 쿠폰으로 생성되고 isLimited()가 false를 반환한다.")
        @Test
        void createsUnlimitedCoupon_whenMaxIssuanceCountIsZero() {
            Coupon coupon = new Coupon("무제한 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, 0);

            assertAll(
                () -> assertThat(coupon.getMaxIssuanceCount()).isZero(),
                () -> assertThat(coupon.isLimited()).isFalse()
            );
        }

        @DisplayName("maxIssuanceCount가 양수이면, 수량 제한 쿠폰으로 생성되고 isLimited()가 true를 반환한다.")
        @Test
        void createsLimitedCoupon_whenMaxIssuanceCountIsPositive() {
            Coupon coupon = new Coupon("선착순 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, 100);

            assertAll(
                () -> assertThat(coupon.getMaxIssuanceCount()).isEqualTo(100),
                () -> assertThat(coupon.isLimited()).isTrue()
            );
        }
    }

    @DisplayName("할인 금액을 계산할 때,")
    @Nested
    class CalculateDiscount {

        @DisplayName("FIXED 타입이면, 정액이 차감된다.")
        @Test
        void calculatesFixedDiscount() {
            Coupon coupon = new Coupon("정액 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, 0);

            BigDecimal discount = coupon.calculateDiscount(BigDecimal.valueOf(10000));

            assertThat(discount).isEqualByComparingTo(BigDecimal.valueOf(1000));
        }

        @DisplayName("RATE 타입이면, 총 금액에 비율을 적용한 할인 금액이 계산된다.")
        @Test
        void calculatesRateDiscount() {
            Coupon coupon = new Coupon("10% 할인 쿠폰", CouponType.RATE, BigDecimal.valueOf(10), null, FUTURE, 0);

            BigDecimal discount = coupon.calculateDiscount(BigDecimal.valueOf(10000));

            assertThat(discount).isEqualByComparingTo(BigDecimal.valueOf(1000));
        }

        @DisplayName("최소 주문 금액 조건을 충족하지 못하면, BAD_REQUEST 예외가 발생한다.")
        @Test
        void throwsBadRequest_whenOrderAmountIsLessThanMinOrderAmount() {
            Coupon coupon = new Coupon("최소주문 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), BigDecimal.valueOf(5000), FUTURE, 0);

            CoreException ex = assertThrows(CoreException.class,
                () -> coupon.calculateDiscount(BigDecimal.valueOf(4999)));
            assertThat(ex.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        }

        @DisplayName("최소 주문 금액이 없는 경우, 금액 조건 없이 할인이 적용된다.")
        @Test
        void calculatesDiscount_whenNoMinOrderAmount() {
            Coupon coupon = new Coupon("무조건 할인 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, 0);

            BigDecimal discount = coupon.calculateDiscount(BigDecimal.valueOf(100));

            assertThat(discount).isEqualByComparingTo(BigDecimal.valueOf(1000));
        }

        @DisplayName("최소 주문 금액과 동일한 금액이면, 할인이 적용된다.")
        @Test
        void calculatesDiscount_whenOrderAmountEqualsMinOrderAmount() {
            Coupon coupon = new Coupon("최소주문 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), BigDecimal.valueOf(5000), FUTURE, 0);

            BigDecimal discount = coupon.calculateDiscount(BigDecimal.valueOf(5000));

            assertThat(discount).isEqualByComparingTo(BigDecimal.valueOf(1000));
        }
    }

    @DisplayName("쿠폰 정보를 수정할 때,")
    @Nested
    class Update {

        @DisplayName("유효한 정보로 수정하면, 정상적으로 수정된다.")
        @Test
        void updatesCoupon_whenValidInfoIsProvided() {
            Coupon coupon = new Coupon("기존 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000), null, FUTURE, 0);
            ZonedDateTime newExpiredAt = FUTURE.plusDays(10);

            coupon.update("수정 쿠폰", CouponType.RATE, BigDecimal.valueOf(20), BigDecimal.valueOf(3000), newExpiredAt, 0);

            assertAll(
                () -> assertThat(coupon.getName()).isEqualTo("수정 쿠폰"),
                () -> assertThat(coupon.getType()).isEqualTo(CouponType.RATE),
                () -> assertThat(coupon.getValue()).isEqualByComparingTo(BigDecimal.valueOf(20)),
                () -> assertThat(coupon.getMinOrderAmount()).isEqualByComparingTo(BigDecimal.valueOf(3000)),
                () -> assertThat(coupon.getExpiredAt()).isEqualTo(newExpiredAt)
            );
        }
    }
}
