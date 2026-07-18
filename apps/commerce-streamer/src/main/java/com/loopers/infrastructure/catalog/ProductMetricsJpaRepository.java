package com.loopers.infrastructure.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * product_metrics upsert 전용 repository. 모든 쿼리가 INSERT ... ON DUPLICATE KEY UPDATE 형태라
 * 행이 없으면 생성하고 있으면 원자적으로 증감시킨다(JPA 변경 감지 대신 네이티브 쿼리를 쓴 이유).
 */
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
    // GREATEST(0, ...)로 좋아요 취소 이벤트가 좋아요 이벤트보다 먼저 도착해도 음수로 내려가지 않게 방어한다.
    void upsertLikeCountDecrement(@Param("productId") Long productId);

    @Modifying
    @Query(value = """
        INSERT INTO product_metrics (product_id, order_count, like_count, view_count, updated_at)
        VALUES (:productId, 0, 0, 1, NOW())
        ON DUPLICATE KEY UPDATE view_count = view_count + 1, updated_at = NOW()
        """, nativeQuery = true)
    void upsertViewCountIncrement(@Param("productId") Long productId);
}
