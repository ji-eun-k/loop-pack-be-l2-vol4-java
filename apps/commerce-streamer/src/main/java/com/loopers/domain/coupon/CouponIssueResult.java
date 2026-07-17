package com.loopers.domain.coupon;

/**
 * 쿠폰 발급 이벤트 처리 결과. DUPLICATE/OUT_OF_STOCK도 실패가 아닌 정상 종결 상태로 취급해
 * 재시도 대상에서 제외한다.
 */
public enum CouponIssueResult {
    PENDING, SUCCESS, DUPLICATE, OUT_OF_STOCK, FAILED
}