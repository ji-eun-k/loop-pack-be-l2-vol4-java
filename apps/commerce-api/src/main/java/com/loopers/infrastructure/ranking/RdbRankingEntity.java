package com.loopers.infrastructure.ranking;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;

@Entity
@Table(
    name = "daily_product_ranking",
    indexes = @Index(
        name = "idx_daily_ranking_date_score_product",
        columnList = "ranking_date, score DESC, product_id DESC"
    )
)
@IdClass(RdbRankingEntity.RankingId.class)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class RdbRankingEntity {

    @Id
    @Column(name = "ranking_date", nullable = false)
    private LocalDate rankingDate;

    @Id
    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "score", nullable = false)
    private double score;

    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class RankingId implements Serializable {
        private LocalDate rankingDate;
        private Long productId;
    }
}
