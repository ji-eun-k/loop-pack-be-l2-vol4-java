package com.loopers.infrastructure.coupon;

import com.loopers.domain.BaseEntity;
import com.loopers.domain.coupon.CouponIssueResult;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import lombok.Getter;

@Getter
@Entity(name = "CouponIssueEvent")
@Table(name = "coupon_issue_event")
public class CouponIssueEventEntity extends BaseEntity {

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "coupon_id", nullable = false)
    private Long couponId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private CouponIssueResult result;

    protected CouponIssueEventEntity() {}

    public CouponIssueEventEntity(String eventId, Long couponId, Long userId, CouponIssueResult result) {
        this.eventId = eventId;
        this.couponId = couponId;
        this.userId = userId;
        this.result = result;
    }

    public void updateResult(CouponIssueResult result) {
        this.result = result;
    }
}