package com.loopers.application.coupon;

import com.loopers.infrastructure.coupon.CouponIssueEventEntity;
import com.loopers.infrastructure.coupon.CouponIssueEventJpaRepository;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@RequiredArgsConstructor
@Component
public class CouponIssueEventService {

    private final CouponIssueEventJpaRepository couponIssueEventJpaRepository;

    @Transactional(readOnly = true)
    public CouponInfo.IssueEvent getIssueEvent(String eventId) {
        CouponIssueEventEntity entity = couponIssueEventJpaRepository.findByEventId(eventId)
            .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "발급 이벤트를 찾을 수 없습니다."));
        return new CouponInfo.IssueEvent(entity.getEventId(), entity.getCouponId(), entity.getUserId(), entity.getResult());
    }
}