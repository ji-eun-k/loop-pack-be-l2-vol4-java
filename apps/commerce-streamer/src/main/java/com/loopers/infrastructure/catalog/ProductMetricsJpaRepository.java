package com.loopers.infrastructure.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProductMetricsJpaRepository extends JpaRepository<ProductMetricsEntity, Long> {

    @Modifying
    @Query(value = """
        INSERT INTO product_metrics (product_id, order_count, like_count, view_count, updated_at)
        VALUES (:productId, :delta, 0, 0, NOW())
        ON DUPLICATE KEY UPDATE order_count = order_count + :delta, updated_at = NOW()
        """, nativeQuery = true)
    void upsertOrderCount(@Param("productId") Long productId, @Param("delta") int delta);

    @Modifying
    @Query(value = """
        INSERT INTO product_metrics (product_id, order_count, like_count, view_count, updated_at)
        VALUES (:productId, 0, 1, 0, NOW())
        ON DUPLICATE KEY UPDATE like_count = like_count + 1, updated_at = NOW()
        """, nativeQuery = true)
    void upsertLikeCountIncrement(@Param("productId") Long productId);

    @Modifying
    @Query(value = """
        INSERT INTO product_metrics (product_id, order_count, like_count, view_count, updated_at)
        VALUES (:productId, 0, 0, 0, NOW())
        ON DUPLICATE KEY UPDATE like_count = GREATEST(0, like_count - 1), updated_at = NOW()
        """, nativeQuery = true)
    void upsertLikeCountDecrement(@Param("productId") Long productId);

    @Modifying
    @Query(value = """
        INSERT INTO product_metrics (product_id, order_count, like_count, view_count, updated_at)
        VALUES (:productId, 0, 0, 1, NOW())
        ON DUPLICATE KEY UPDATE view_count = view_count + 1, updated_at = NOW()
        """, nativeQuery = true)
    void upsertViewCountIncrement(@Param("productId") Long productId);
}
