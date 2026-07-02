package com.loopers.infrastructure.coupon;

import com.loopers.domain.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;

import java.time.ZonedDateTime;

@Getter
@Entity(name = "IssuedCoupon")
@Table(name = "issued_coupon")
public class IssuedCouponEntity extends BaseEntity {

    @Version
    private Long version;

    @Column(name = "coupon_id", nullable = false)
    private Long couponId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(nullable = false)
    private String status;

    @Column(name = "expired_at", nullable = false)
    private ZonedDateTime expiredAt;

    protected IssuedCouponEntity() {}

    public IssuedCouponEntity(Long couponId, Long userId, ZonedDateTime expiredAt) {
        this.couponId = couponId;
        this.userId = userId;
        this.status = "AVAILABLE";
        this.expiredAt = expiredAt;
    }
}
