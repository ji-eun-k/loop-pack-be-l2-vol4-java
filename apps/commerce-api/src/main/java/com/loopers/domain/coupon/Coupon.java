package com.loopers.domain.coupon;

import com.loopers.domain.BaseDomain;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.Getter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZonedDateTime;

@Getter
public class Coupon extends BaseDomain {

    private String name;
    private CouponType type;
    private BigDecimal value;
    private BigDecimal minOrderAmount;
    private ZonedDateTime expiredAt;
    private int maxIssuanceCount;
    private int issuedCount;

    public Coupon(String name, CouponType type, BigDecimal value, BigDecimal minOrderAmount, ZonedDateTime expiredAt, int maxIssuanceCount) {
        validate(name, type, value, expiredAt, maxIssuanceCount);
        this.name = name;
        this.type = type;
        this.value = value;
        this.minOrderAmount = minOrderAmount;
        this.expiredAt = expiredAt;
        this.maxIssuanceCount = maxIssuanceCount;
        this.issuedCount = 0;
    }

    public Coupon(Long id, String name, CouponType type, BigDecimal value, BigDecimal minOrderAmount,
                  ZonedDateTime expiredAt, int maxIssuanceCount, int issuedCount,
                  ZonedDateTime createdAt, ZonedDateTime updatedAt, ZonedDateTime deletedAt) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.value = value;
        this.minOrderAmount = minOrderAmount;
        this.expiredAt = expiredAt;
        this.maxIssuanceCount = maxIssuanceCount;
        this.issuedCount = issuedCount;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.deletedAt = deletedAt;
    }

    public boolean isLimited() {
        return maxIssuanceCount > 0;
    }

    public BigDecimal calculateDiscount(BigDecimal orderAmount) {
        if (minOrderAmount != null && orderAmount.compareTo(minOrderAmount) < 0) {
            throw new CoreException(ErrorType.BAD_REQUEST, "최소 주문 금액 조건을 충족하지 못했습니다.");
        }
        if (type == CouponType.FIXED) {
            return value;
        }
        // RATE: orderAmount * value(%) / 100, 소수점 이하 버림
        return orderAmount.multiply(value).divide(BigDecimal.valueOf(100), 0, RoundingMode.DOWN);
    }

    public void update(String name, CouponType type, BigDecimal value, BigDecimal minOrderAmount, ZonedDateTime expiredAt, int maxIssuanceCount) {
        validate(name, type, value, expiredAt, maxIssuanceCount);
        this.name = name;
        this.type = type;
        this.value = value;
        this.minOrderAmount = minOrderAmount;
        this.expiredAt = expiredAt;
        this.maxIssuanceCount = maxIssuanceCount;
    }

    private void validate(String name, CouponType type, BigDecimal value, ZonedDateTime expiredAt, int maxIssuanceCount) {
        if (name == null || name.isBlank()) {
            throw new CoreException(ErrorType.BAD_REQUEST, "쿠폰 이름은 필수입니다.");
        }
        if (type == null) {
            throw new CoreException(ErrorType.BAD_REQUEST, "쿠폰 타입은 필수입니다.");
        }
        if (value == null) {
            throw new CoreException(ErrorType.BAD_REQUEST, "쿠폰 할인 값은 필수입니다.");
        }
        if (type == CouponType.FIXED && value.compareTo(BigDecimal.ZERO) <= 0) {
            throw new CoreException(ErrorType.BAD_REQUEST, "정액 쿠폰의 할인 값은 0보다 커야 합니다.");
        }
        if (type == CouponType.RATE
            && (value.compareTo(BigDecimal.ONE) < 0 || value.compareTo(BigDecimal.valueOf(100)) > 0)) {
            throw new CoreException(ErrorType.BAD_REQUEST, "정률 쿠폰의 할인율은 1 이상 100 이하여야 합니다.");
        }
        if (expiredAt == null) {
            throw new CoreException(ErrorType.BAD_REQUEST, "쿠폰 만료일은 필수입니다.");
        }
        if (maxIssuanceCount < 0) {
            throw new CoreException(ErrorType.BAD_REQUEST, "최대 발급 수량은 0 이상이어야 합니다.");
        }
    }
}
