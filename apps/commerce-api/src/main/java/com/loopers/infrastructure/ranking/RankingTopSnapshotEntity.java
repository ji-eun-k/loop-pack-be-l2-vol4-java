package com.loopers.infrastructure.ranking;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZonedDateTime;

@Entity(name = "RankingTopSnapshot")
@Table(
    name = "ranking_top_snapshot",
    indexes = @Index(name = "idx_ranking_snapshot_date_product", columnList = "ranking_date, product_id")
)
@IdClass(RankingTopSnapshotEntity.SnapshotId.class)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class RankingTopSnapshotEntity {

    @Id
    @Column(name = "ranking_date", nullable = false)
    private LocalDate rankingDate;

    @Id
    @Column(name = "rank_position", nullable = false)
    private Integer rankPosition;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "score", nullable = false)
    private Double score;

    @Column(name = "snapshotted_at", nullable = false)
    private ZonedDateTime snapshottedAt;

    @NoArgsConstructor
    public static class SnapshotId implements Serializable {
        private LocalDate rankingDate;
        private Integer rankPosition;
    }
}
