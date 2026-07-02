package com.loopers.application.coupon;

public record CouponStockInitEvent(Long couponId, int maxIssuanceCount) {}
