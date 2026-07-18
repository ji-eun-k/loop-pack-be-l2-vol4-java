package com.loopers.infrastructure.coupon;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface IssuedCouponJpaRepository extends JpaRepository<IssuedCouponEntity, Long> {

    boolean existsByCouponIdAndUserId(Long couponId, Long userId);

    long countByCouponIdAndUserId(Long couponId, Long userId);

    // coupon 테이블(다른 앱이 소유하는 도메인)의 issued_count를 직접 증가시켜 총 발급 수를 동기화한다.
    @Modifying
    @Query(value = "UPDATE coupon SET issued_count = issued_count + 1 WHERE id = :couponId", nativeQuery = true)
    void incrementIssuedCount(@Param("couponId") Long couponId);
}
