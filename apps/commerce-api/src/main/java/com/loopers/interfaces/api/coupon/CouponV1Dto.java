package com.loopers.interfaces.api.coupon;

import com.loopers.application.coupon.CouponInfo;
import com.loopers.domain.coupon.CouponIssueResult;
import com.loopers.domain.coupon.CouponStatus;
import com.loopers.domain.coupon.CouponType;

import java.time.ZonedDateTime;

public class CouponV1Dto {

    public record IssueResponse(String eventId) {
        public static IssueResponse from(String eventId) {
            return new IssueResponse(eventId);
        }
    }

    public record IssueStatusResponse(
            String eventId,
            Long couponId,
            Long userId,
            CouponIssueResult result
    ) {
        public static IssueStatusResponse from(CouponInfo.IssueEvent info) {
            return new IssueStatusResponse(info.eventId(), info.couponId(), info.userId(), info.result());
        }
    }

    public record MyCouponResponse(
            Long issuedCouponId,
            Long couponId,
            ZonedDateTime expiredAt,
            CouponStatus status
    ) {
        public static MyCouponResponse from(CouponInfo.MyCoupon info) {
            return new MyCouponResponse(
                info.issuedCouponId(), info.couponId(),
                info.expiredAt(), info.status()
            );
        }
    }
}
