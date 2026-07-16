package com.loopers.application.ranking;

import com.loopers.domain.catalog.CatalogLedgerEvent;
import com.loopers.infrastructure.catalog.CatalogEventLedgerWriter;
import com.loopers.testcontainers.KafkaTestContainersConfig;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "ranking.recovery.enabled=false")
@Import({
    MySqlTestContainersConfig.class,
    RedisTestContainersConfig.class,
    KafkaTestContainersConfig.class,
    RankingRecoveryServiceIntegrationTest.FixedClockConfig.class
})
class RankingRecoveryServiceIntegrationTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final LocalDate DATE = LocalDate.of(2026, 7, 17);
    private static final Instant NOW = DATE.atTime(12, 0).atZone(SEOUL).toInstant();
    private static final String RANKING_KEY = "ranking:all:20260717";

    @TestConfiguration
    static class FixedClockConfig {
        @Primary
        @Bean
        Clock fixedClock() {
            return Clock.fixed(NOW, SEOUL);
        }
    }

    @Autowired private CatalogEventLedgerWriter ledgerWriter;
    @Autowired private RankingRecoveryService recoveryService;
    @Autowired private RedisTemplate<String, String> redisTemplate;
    @Autowired private DatabaseCleanUp databaseCleanUp;
    @Autowired private RedisCleanUp redisCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        redisCleanUp.truncateAll();
    }

    @Test
    @DisplayName("Redis 데이터가 유실되면 오늘 원장 이벤트를 재생해 랭킹을 복구한다")
    void recoversTodayRankingFromLedger() {
        ledgerWriter.appendAll(List.of(
            event("view-event", "ProductViewedEvent", 0, "{\"productId\":1001}"),
            event("like-event", "ProductLikedEvent", 1, "{\"productId\":1001}")
        ));

        RankingRecoveryService.RecoveryResult result = recoveryService.recoverTodayIfNecessary();

        assertThat(result.status()).isEqualTo(RankingRecoveryService.Status.COMPLETED);
        assertThat(result.scanned()).isEqualTo(2);
        assertThat(result.applied()).isEqualTo(2);
        assertThat(redisTemplate.opsForZSet().score(RANKING_KEY, "1001")).isCloseTo(0.3,
            org.assertj.core.data.Offset.offset(1e-9));
        assertThat(redisTemplate.opsForSet().isMember("ranking:handled:20260717", "view-event")).isTrue();
        assertThat(redisTemplate.opsForSet().isMember("ranking:handled:20260717", "like-event")).isTrue();
    }

    @Test
    @DisplayName("복구 완료 마커가 있으면 원장을 다시 조회해 중복 반영하지 않는다")
    void skipsWhenRecoveryAlreadyCompleted() {
        ledgerWriter.appendAll(List.of(event("view-event", "ProductViewedEvent", 0,
            "{\"productId\":1001}")));

        RankingRecoveryService.RecoveryResult first = recoveryService.recoverTodayIfNecessary();
        RankingRecoveryService.RecoveryResult second = recoveryService.recoverTodayIfNecessary();

        assertThat(first.status()).isEqualTo(RankingRecoveryService.Status.COMPLETED);
        assertThat(second.status()).isEqualTo(RankingRecoveryService.Status.ALREADY_COMPLETED);
        assertThat(redisTemplate.opsForZSet().score(RANKING_KEY, "1001")).isEqualTo(0.1);
    }

    @Test
    @DisplayName("다른 파드가 복구 락을 보유하면 현재 파드는 건너뛴다")
    void skipsWhenAnotherPodOwnsLock() {
        redisTemplate.opsForValue().set("ranking:recovery:lock:2026-07-17", "other-pod");

        RankingRecoveryService.RecoveryResult result = recoveryService.recoverTodayIfNecessary();

        assertThat(result.status()).isEqualTo(RankingRecoveryService.Status.LOCKED_BY_ANOTHER_POD);
    }

    private CatalogLedgerEvent event(String eventId, String eventType, long offset, String payload) {
        return new CatalogLedgerEvent(eventId, eventType, NOW, NOW, payload,
            "catalog-events-v1", 0, offset, 1);
    }
}
