package com.loopers.infrastructure.catalog;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.ZonedDateTime;

/**
 * 상품별 누적 지표(주문/좋아요/조회수). 실제 증감은 ProductMetricsJpaRepository의 upsert 쿼리로
 * 이루어지며, 이 엔티티는 조회 용도로만 주로 사용된다.
 */
@Getter
@Entity(name = "ProductMetrics")
@Table(name = "product_metrics")
public class ProductMetricsEntity {

    @Id
    @Column(name = "product_id")
    private Long productId;

    @Column(name = "order_count", nullable = false)
    private Long orderCount = 0L;

    @Column(name = "like_count", nullable = false)
    private Long likeCount = 0L;

    @Column(name = "view_count", nullable = false)
    private Long viewCount = 0L;

    @Column(name = "updated_at", nullable = false)
    private ZonedDateTime updatedAt;

    protected ProductMetricsEntity() {}

    @PrePersist
    @PreUpdate
    private void updateTimestamp() {
        this.updatedAt = ZonedDateTime.now();
    }
}
