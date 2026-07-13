package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.testcontainers.KafkaTestContainersConfig;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import({MySqlTestContainersConfig.class, RedisTestContainersConfig.class, KafkaTestContainersConfig.class})
class RankingUpdaterIntegrationTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final LocalDate FIXED_DATE = LocalDate.of(2026, 7, 12);
    private static final String TODAY_KEY = "ranking:all:20260712";

    @TestConfiguration
    static class FixedClockConfig {
        @Primary
        @Bean
        Clock fixedClock() {
            return Clock.fixed(FIXED_DATE.atStartOfDay(SEOUL).toInstant(), SEOUL);
        }
    }

    @DisplayName("applyEvents()를 실행할 때,")
    @Nested
    class ApplyEvents {

        @DisplayName("동일한 eventId를 재처리해도 점수는 한 번만 적립한다.")
        @Test
        void appliesSameEventOnlyOnce() {
            RankingEventScore event = new RankingEventScore("event-1", Map.of(1001L, 0.2));

            rankingUpdater.applyEvents(FIXED_DATE, List.of(event));
            rankingUpdater.applyEvents(FIXED_DATE, List.of(event));

            assertThat(redisTemplate.opsForZSet().score(TODAY_KEY, "1001")).isEqualTo(0.2);
        }

        @DisplayName("처리 시각과 무관하게 전달받은 이벤트 날짜 키에 적립한다.")
        @Test
        void appliesScoreToEventDate() {
            LocalDate previousDate = FIXED_DATE.minusDays(1);

            rankingUpdater.applyEvents(previousDate,
                List.of(new RankingEventScore("event-2", Map.of(1001L, 0.1))));

            assertThat(redisTemplate.opsForZSet().score("ranking:all:20260711", "1001")).isEqualTo(0.1);
            assertThat(redisTemplate.opsForZSet().score(TODAY_KEY, "1001")).isNull();
        }
    }

    @Autowired
    private RankingUpdater rankingUpdater;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private RedisCleanUp redisCleanUp;

    @AfterEach
    void tearDown() {
        redisCleanUp.truncateAll();
    }

    @DisplayName("applyDeltas()를 실행할 때,")
    @Nested
    class ApplyDeltas {

        @DisplayName("오늘 날짜 키의 ZSET에 상품별 델타 점수를 반영한다.")
        @Test
        void appliesDeltas_toTodayRankingKey() {
            Map<Long, Double> deltas = Map.of(1001L, 0.1, 1002L, 1.8);

            rankingUpdater.applyDeltas(deltas);

            assertThat(redisTemplate.opsForZSet().score(TODAY_KEY, "1001")).isEqualTo(0.1);
            assertThat(redisTemplate.opsForZSet().score(TODAY_KEY, "1002")).isEqualTo(1.8);
        }

        @DisplayName("기존 점수가 있으면 델타를 누적한다.")
        @Test
        void accumulatesDeltas_whenScoreAlreadyExists() {
            rankingUpdater.applyDeltas(Map.of(1001L, 0.3));

            rankingUpdater.applyDeltas(Map.of(1001L, 0.2));

            assertThat(redisTemplate.opsForZSet().score(TODAY_KEY, "1001")).isEqualTo(0.5);
        }

        @DisplayName("음수 델타(좋아요 취소)면 점수를 차감한다.")
        @Test
        void decreasesScore_whenDeltaIsNegative() {
            rankingUpdater.applyDeltas(Map.of(1001L, 0.4));

            rankingUpdater.applyDeltas(Map.of(1001L, -0.2));

            assertThat(redisTemplate.opsForZSet().score(TODAY_KEY, "1001")).isCloseTo(0.2, org.assertj.core.data.Offset.offset(1e-9));
        }

        @DisplayName("키에 TTL 2일이 설정된다.")
        @Test
        void setsTwoDaysTtl_onRankingKey() {
            rankingUpdater.applyDeltas(Map.of(1001L, 0.1));

            Long ttlSeconds = redisTemplate.getExpire(TODAY_KEY, TimeUnit.SECONDS);

            assertThat(ttlSeconds).isNotNull();
            assertThat(ttlSeconds).isGreaterThan(TimeUnit.DAYS.toSeconds(2) - 60)
                .isLessThanOrEqualTo(TimeUnit.DAYS.toSeconds(2));
        }

        @DisplayName("델타가 비어 있으면 키를 생성하지 않는다.")
        @Test
        void doesNothing_whenDeltasEmpty() {
            rankingUpdater.applyDeltas(Map.of());

            assertThat(redisTemplate.hasKey(TODAY_KEY)).isFalse();
        }
    }

    @DisplayName("carryOverToNextDay()를 실행할 때,")
    @Nested
    class CarryOverToNextDay {

        private static final String TOMORROW_KEY = "ranking:all:20260713";

        @DisplayName("오늘 점수의 0.1배로 내일 키를 미리 생성한다 (콜드 스타트 방지).")
        @Test
        void createsTomorrowKey_withTenPercentOfTodayScores() {
            redisTemplate.opsForZSet().add(TODAY_KEY, "1001", 80.0);
            redisTemplate.opsForZSet().add(TODAY_KEY, "1002", 30.0);

            rankingUpdater.carryOverToNextDay();

            assertThat(redisTemplate.opsForZSet().score(TOMORROW_KEY, "1001")).isEqualTo(8.0);
            assertThat(redisTemplate.opsForZSet().score(TOMORROW_KEY, "1002")).isEqualTo(3.0);
        }

        @DisplayName("내일 키에 TTL 2일이 설정된다.")
        @Test
        void setsTwoDaysTtl_onTomorrowKey() {
            redisTemplate.opsForZSet().add(TODAY_KEY, "1001", 10.0);

            rankingUpdater.carryOverToNextDay();

            Long ttlSeconds = redisTemplate.getExpire(TOMORROW_KEY, TimeUnit.SECONDS);
            assertThat(ttlSeconds).isNotNull();
            assertThat(ttlSeconds).isGreaterThan(TimeUnit.DAYS.toSeconds(2) - 60)
                .isLessThanOrEqualTo(TimeUnit.DAYS.toSeconds(2));
        }

        @DisplayName("오늘 랭킹이 없으면 내일 키를 생성하지 않는다.")
        @Test
        void doesNothing_whenTodayRankingIsEmpty() {
            rankingUpdater.carryOverToNextDay();

            assertThat(redisTemplate.hasKey(TOMORROW_KEY)).isFalse();
        }

        @DisplayName("재실행해도 오늘 점수 기준으로 재계산되어 결과가 같다 (멱등).")
        @Test
        void isIdempotent_whenExecutedTwice() {
            redisTemplate.opsForZSet().add(TODAY_KEY, "1001", 50.0);

            rankingUpdater.carryOverToNextDay();
            rankingUpdater.carryOverToNextDay();

            assertThat(redisTemplate.opsForZSet().score(TOMORROW_KEY, "1001")).isEqualTo(5.0);
        }
    }
}
