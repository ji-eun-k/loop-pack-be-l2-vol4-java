package com.loopers.application.ranking;

import com.loopers.domain.ranking.RankingItem;
import com.loopers.infrastructure.ranking.RankingTopSnapshotRepository;
import com.loopers.support.ranking.RankingKeys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class RankingFallbackSnapshotScheduler {

    private final RedisTemplate<String, String> redisTemplate;
    private final RankingTopSnapshotRepository snapshotRepository;
    private final RankingFallbackSnapshotProperties properties;
    private final Clock clock;

    @Scheduled(fixedDelayString = "${ranking.fallback-snapshot.interval-ms:600000}")
    public void snapshotTodayTopRanking() {
        LocalDate today = LocalDate.now(clock);
        try {
            Set<ZSetOperations.TypedTuple<String>> tuples = redisTemplate.opsForZSet()
                .reverseRangeWithScores(RankingKeys.daily(today), 0, properties.topN() - 1L);
            if (tuples == null || tuples.isEmpty()) {
                return;
            }

            List<RankingItem> items = tuples.stream()
                .filter(tuple -> tuple.getValue() != null && tuple.getScore() != null)
                .map(tuple -> new RankingItem(Long.parseLong(tuple.getValue()), tuple.getScore()))
                .toList();
            if (!items.isEmpty()) {
                snapshotRepository.replace(today, items, ZonedDateTime.now(clock));
            }
        } catch (RuntimeException exception) {
            log.warn("[RANKING] Top-N fallback snapshot 갱신 실패 — 기존 스냅샷 유지, date={}", today, exception);
        }
    }
}
