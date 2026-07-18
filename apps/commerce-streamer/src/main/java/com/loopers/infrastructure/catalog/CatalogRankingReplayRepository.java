package com.loopers.infrastructure.catalog;

import com.loopers.domain.ranking.CatalogRankingReplayEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * RankingRecoveryService가 랭킹을 리플레이할 때 원장 테이블을 커서 기반으로 페이징 조회하는 저장소.
 */
@Repository
@RequiredArgsConstructor
public class CatalogRankingReplayRepository {

    private final JdbcTemplate jdbcTemplate;

    public List<CatalogRankingReplayEvent> findBatch(
        Instant fromInclusive,
        Instant toExclusive,
        long lastLedgerId,
        int batchSize
    ) {
        return jdbcTemplate.query(
            """
                SELECT id, event_id, event_type, occurred_at, payload
                FROM catalog_event_ledger
                WHERE occurred_at >= ?
                  AND occurred_at < ?
                  AND id > ?
                ORDER BY id
                LIMIT ?
                """,
            (resultSet, rowNumber) -> new CatalogRankingReplayEvent(
                resultSet.getLong("id"),
                resultSet.getString("event_id"),
                resultSet.getString("event_type"),
                resultSet.getTimestamp("occurred_at").toInstant(),
                resultSet.getString("payload")
            ),
            Timestamp.from(fromInclusive),
            Timestamp.from(toExclusive),
            lastLedgerId,
            batchSize
        );
    }
}
