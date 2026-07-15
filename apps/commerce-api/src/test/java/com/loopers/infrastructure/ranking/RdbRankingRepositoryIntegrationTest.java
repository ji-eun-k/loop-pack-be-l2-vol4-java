package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingItem;
import com.loopers.domain.ranking.RankingRepository;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "ranking.repository=rdb")
class RdbRankingRepositoryIntegrationTest {

    private static final LocalDate DATE = LocalDate.of(2026, 7, 12);

    @Autowired private RankingRepository rankingRepository;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    void readsPageRankAndCountFromRdb() {
        jdbcTemplate.batchUpdate(
            "INSERT INTO daily_product_ranking (ranking_date, product_id, score) VALUES (?, ?, ?)",
            List.of(
                new Object[]{DATE, 101L, 87.3}, new Object[]{DATE, 102L, 55.0},
                new Object[]{DATE, 103L, 30.5}, new Object[]{DATE, 104L, 12.0},
                new Object[]{DATE, 105L, 3.1}
            )
        );

        List<RankingItem> secondPage = rankingRepository.findPage(DATE, 1, 3);

        assertThat(secondPage).extracting(RankingItem::productId).containsExactly(104L, 105L);
        assertThat(rankingRepository.findRank(DATE, 103L)).contains(3L);
        assertThat(rankingRepository.findRank(DATE, 999L)).isEmpty();
        assertThat(rankingRepository.countTotal(DATE)).isEqualTo(5L);
    }
}
