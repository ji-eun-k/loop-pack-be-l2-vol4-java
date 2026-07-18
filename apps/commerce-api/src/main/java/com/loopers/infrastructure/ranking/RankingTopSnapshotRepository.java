package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingItem;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class RankingTopSnapshotRepository {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public void replace(LocalDate date, List<RankingItem> items, ZonedDateTime snapshottedAt) {
        jdbcTemplate.update("DELETE FROM ranking_top_snapshot WHERE ranking_date = ?", date);
        List<Integer> indexes = java.util.stream.IntStream.range(0, items.size()).boxed().toList();
        jdbcTemplate.batchUpdate(
            """
                INSERT INTO ranking_top_snapshot
                    (ranking_date, rank_position, product_id, score, snapshotted_at)
                VALUES (?, ?, ?, ?, ?)
                """,
            indexes,
            indexes.size(),
            (statement, index) -> {
                RankingItem item = items.get(index);
                statement.setDate(1, Date.valueOf(date));
                statement.setInt(2, index + 1);
                statement.setLong(3, item.productId());
                statement.setDouble(4, item.score());
                statement.setTimestamp(5, Timestamp.from(snapshottedAt.toInstant()));
            }
        );
    }

    public List<RankingItem> findPage(LocalDate date, int page, int size) {
        int offset = Math.multiplyExact(page, size);
        return jdbcTemplate.query(
            """
                SELECT product_id, score
                FROM ranking_top_snapshot
                WHERE ranking_date = ?
                ORDER BY rank_position
                LIMIT ? OFFSET ?
                """,
            (rs, rowNum) -> new RankingItem(rs.getLong("product_id"), rs.getDouble("score")),
            date, size, offset
        );
    }

    public Optional<Long> findRank(LocalDate date, Long productId) {
        return jdbcTemplate.query(
            "SELECT rank_position FROM ranking_top_snapshot WHERE ranking_date = ? AND product_id = ?",
            (rs, rowNum) -> rs.getLong("rank_position"),
            date, productId
        ).stream().findFirst();
    }

    public long count(LocalDate date) {
        Long count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM ranking_top_snapshot WHERE ranking_date = ?", Long.class, date
        );
        return count == null ? 0L : count;
    }
}
