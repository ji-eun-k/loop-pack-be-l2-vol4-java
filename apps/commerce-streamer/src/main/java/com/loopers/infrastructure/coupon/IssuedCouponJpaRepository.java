package com.loopers.infrastructure.coupon;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface IssuedCouponJpaRepository extends JpaRepository<IssuedCouponEntity, Long> {

    boolean existsByCouponIdAndUserId(Long couponId, Long userId);

    @Modifying
    @Query(value = "UPDATE coupon SET issued_count = issued_count + 1 WHERE id = :couponId", nativeQuery = true)
    void incrementIssuedCount(@Param("couponId") Long couponId);
}
