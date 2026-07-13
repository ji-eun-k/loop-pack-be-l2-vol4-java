package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingItem;
import com.loopers.domain.ranking.RankingRepository;
import com.loopers.support.ranking.RankingKeys;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(RedisTestContainersConfig.class)
class RedisRankingRepositoryIntegrationTest {

    private static final LocalDate DATE = LocalDate.of(2026, 7, 12);

    @Autowired
    private RankingRepository rankingRepository;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private RedisCleanUp redisCleanUp;

    @BeforeEach
    void seedRanking() {
        String key = RankingKeys.daily(DATE);
        redisTemplate.opsForZSet().add(key, "101", 87.3);
        redisTemplate.opsForZSet().add(key, "102", 55.0);
        redisTemplate.opsForZSet().add(key, "103", 30.5);
        redisTemplate.opsForZSet().add(key, "104", 12.0);
        redisTemplate.opsForZSet().add(key, "105", 3.1);
    }

    @AfterEach
    void tearDown() {
        redisCleanUp.truncateAll();
    }

    @DisplayName("findPage()로 랭킹 페이지를 조회할 때,")
    @Nested
    class FindPage {

        @DisplayName("점수 내림차순으로 첫 페이지를 반환한다.")
        @Test
        void returnsFirstPage_orderedByScoreDesc() {
            List<RankingItem> items = rankingRepository.findPage(DATE, 0, 3);

            assertThat(items).extracting(RankingItem::productId).containsExactly(101L, 102L, 103L);
            assertThat(items.get(0).score()).isEqualTo(87.3);
        }

        @DisplayName("두 번째 페이지는 offset 이후 항목을 반환한다.")
        @Test
        void returnsSecondPage_afterOffset() {
            List<RankingItem> items = rankingRepository.findPage(DATE, 1, 3);

            assertThat(items).extracting(RankingItem::productId).containsExactly(104L, 105L);
        }

        @DisplayName("데이터가 없는 날짜면 빈 리스트를 반환한다.")
        @Test
        void returnsEmptyList_whenDateHasNoRanking() {
            List<RankingItem> items = rankingRepository.findPage(DATE.plusDays(5), 0, 3);

            assertThat(items).isEmpty();
        }
    }

    @DisplayName("findRank()로 상품 순위를 조회할 때,")
    @Nested
    class FindRank {

        @DisplayName("점수가 가장 높은 상품은 1위를 반환한다 (1-based).")
        @Test
        void returnsRankOne_forHighestScoredProduct() {
            Optional<Long> rank = rankingRepository.findRank(DATE, 101L);

            assertThat(rank).contains(1L);
        }

        @DisplayName("세 번째로 높은 상품은 3위를 반환한다.")
        @Test
        void returnsRankThree_forThirdProduct() {
            Optional<Long> rank = rankingRepository.findRank(DATE, 103L);

            assertThat(rank).contains(3L);
        }

        @DisplayName("랭킹에 없는 상품이면 빈 Optional을 반환한다.")
        @Test
        void returnsEmpty_whenProductNotRanked() {
            Optional<Long> rank = rankingRepository.findRank(DATE, 999L);

            assertThat(rank).isEmpty();
        }
    }

    @DisplayName("countTotal()로 전체 랭킹 수를 조회할 때,")
    @Nested
    class CountTotal {

        @DisplayName("ZSET 전체 멤버 수를 반환한다.")
        @Test
        void returnsTotalCount() {
            long total = rankingRepository.countTotal(DATE);

            assertThat(total).isEqualTo(5L);
        }

        @DisplayName("데이터가 없는 날짜면 0을 반환한다.")
        @Test
        void returnsZero_whenDateHasNoRanking() {
            long total = rankingRepository.countTotal(DATE.plusDays(5));

            assertThat(total).isEqualTo(0L);
        }
    }
}
