package com.loopers.infrastructure.ranking;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;

@Entity
@Table(
    name = "daily_ranking_handled_event",
    indexes = @Index(name = "idx_ranking_handled_event_date", columnList = "ranking_date")
)
@IdClass(RdbRankingHandledEventEntity.HandledEventId.class)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class RdbRankingHandledEventEntity {

    @Id
    @Column(name = "ranking_date", nullable = false)
    private LocalDate rankingDate;

    @Id
    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class HandledEventId implements Serializable {
        private LocalDate rankingDate;
        private String eventId;
    }
}
