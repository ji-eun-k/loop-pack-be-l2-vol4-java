package com.loopers.infrastructure.coupon;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface CouponIssueEventJpaRepository extends JpaRepository<CouponIssueEventEntity, Long> {
    Optional<CouponIssueEventEntity> findByEventId(String eventId);
}