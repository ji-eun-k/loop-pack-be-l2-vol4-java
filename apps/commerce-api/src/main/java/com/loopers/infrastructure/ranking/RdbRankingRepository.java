package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingItem;
import com.loopers.domain.ranking.RankingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
@ConditionalOnProperty(name = "ranking.repository", havingValue = "rdb")
public class RdbRankingRepository implements RankingRepository {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<RankingItem> findPage(LocalDate date, int page, int size) {
        int offset = Math.multiplyExact(page, size);
        return jdbcTemplate.query(
            """
                SELECT product_id, score
                FROM daily_product_ranking
                WHERE ranking_date = ?
                ORDER BY score DESC, product_id DESC
                LIMIT ? OFFSET ?
                """,
            (rs, rowNum) -> new RankingItem(rs.getLong("product_id"), rs.getDouble("score")),
            date, size, offset
        );
    }

    @Override
    public Optional<Long> findRank(LocalDate date, Long productId) {
        List<Long> ranks = jdbcTemplate.query(
            """
                SELECT ranked.ranking_position
                FROM (
                    SELECT product_id,
                           ROW_NUMBER() OVER (ORDER BY score DESC, product_id DESC) AS ranking_position
                    FROM daily_product_ranking
                    WHERE ranking_date = ?
                ) ranked
                WHERE ranked.product_id = ?
                """,
            (rs, rowNum) -> rs.getLong("ranking_position"),
            date, productId
        );
        return ranks.stream().findFirst();
    }

    @Override
    public long countTotal(LocalDate date) {
        Long count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM daily_product_ranking WHERE ranking_date = ?",
            Long.class,
            date
        );
        return count == null ? 0L : count;
    }
}
