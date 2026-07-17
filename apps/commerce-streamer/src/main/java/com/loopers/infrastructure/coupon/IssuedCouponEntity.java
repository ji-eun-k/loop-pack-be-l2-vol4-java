package com.loopers.infrastructure.coupon;

import com.loopers.domain.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Getter;

import java.time.ZonedDateTime;

/**
 * 유저에게 발급된 쿠폰 1건. coupon_id+user_id 조합의 존재 여부로 중복 발급을 판단한다.
 */
@Getter
@Entity(name = "IssuedCoupon")
@Table(name = "issued_coupon")
public class IssuedCouponEntity extends BaseEntity {

    // 동시에 같은 발급 건을 갱신하는 경쟁 상황에서 낙관적 락으로 덮어쓰기를 방지한다.
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
