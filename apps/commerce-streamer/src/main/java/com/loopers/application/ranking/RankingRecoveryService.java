package com.loopers.application.ranking;

import com.loopers.domain.ranking.CatalogRankingReplayEvent;
import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.infrastructure.catalog.CatalogRankingReplayRepository;
import com.loopers.infrastructure.ranking.RankingUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class RankingRecoveryService {

    private static final String LOCK_PREFIX = "ranking:recovery:lock:";
    private static final String COMPLETED_PREFIX = "ranking:recovery:completed:";
    private static final DefaultRedisScript<Long> RELEASE_LOCK_SCRIPT = new DefaultRedisScript<>("""
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """, Long.class);

    private final CatalogRankingReplayRepository replayRepository;
    private final CatalogRankingScoreMapper scoreMapper;
    private final RankingUpdater rankingUpdater;
    private final RankingRecoveryProperties properties;
    private final RedisTemplate<String, String> redisTemplate;
    private final Clock clock;

    public RecoveryResult recoverTodayIfNecessary() {
        return recoverIfNecessary(LocalDate.now(clock));
    }

    public RecoveryResult recoverIfNecessary(LocalDate date) {
        String completedKey = COMPLETED_PREFIX + date;
        if (Boolean.TRUE.equals(redisTemplate.hasKey(completedKey))) {
            return RecoveryResult.alreadyCompleted(date);
        }

        String lockKey = LOCK_PREFIX + date;
        String owner = UUID.randomUUID().toString();
        Boolean locked = redisTemplate.opsForValue().setIfAbsent(lockKey, owner, properties.lockTtl());
        if (!Boolean.TRUE.equals(locked)) {
            return RecoveryResult.locked(date);
        }

        try {
            if (Boolean.TRUE.equals(redisTemplate.hasKey(completedKey))) {
                return RecoveryResult.alreadyCompleted(date);
            }
            RecoveryResult result = replay(date);
            redisTemplate.opsForValue().set(completedKey, "1", properties.markerTtl());
            return result;
        } finally {
            redisTemplate.execute(RELEASE_LOCK_SCRIPT, List.of(lockKey), owner);
        }
    }

    private RecoveryResult replay(LocalDate date) {
        Instant from = date.atStartOfDay(clock.getZone()).toInstant();
        Instant to = date.plusDays(1).atStartOfDay(clock.getZone()).toInstant();
        long lastLedgerId = 0L;
        long scanned = 0L;
        long applied = 0L;

        while (true) {
            List<CatalogRankingReplayEvent> batch = replayRepository.findBatch(
                from, to, lastLedgerId, properties.batchSize()
            );
            if (batch.isEmpty()) {
                break;
            }

            List<RankingEventScore> scores = new ArrayList<>();
            for (CatalogRankingReplayEvent event : batch) {
                try {
                    Map<Long, Double> deltas = scoreMapper.map(event.eventType(), event.payload());
                    if (!deltas.isEmpty()) {
                        scores.add(new RankingEventScore(event.eventId(), deltas));
                    }
                } catch (Exception exception) {
                    throw new IllegalStateException("ranking replay mapping failed. eventId=" + event.eventId(), exception);
                }
            }
            applied += rankingUpdater.applyEvents(date, scores);
            scanned += batch.size();
            lastLedgerId = batch.get(batch.size() - 1).ledgerId();
        }

        log.info("[RANKING_RECOVERY] ledger replay completed. date={}, scanned={}, applied={}",
            date, scanned, applied);
        return RecoveryResult.completed(date, scanned, applied);
    }

    public record RecoveryResult(LocalDate date, Status status, long scanned, long applied) {
        public static RecoveryResult completed(LocalDate date, long scanned, long applied) {
            return new RecoveryResult(date, Status.COMPLETED, scanned, applied);
        }

        public static RecoveryResult alreadyCompleted(LocalDate date) {
            return new RecoveryResult(date, Status.ALREADY_COMPLETED, 0, 0);
        }

        public static RecoveryResult locked(LocalDate date) {
            return new RecoveryResult(date, Status.LOCKED_BY_ANOTHER_POD, 0, 0);
        }
    }

    public enum Status {
        COMPLETED,
        ALREADY_COMPLETED,
        LOCKED_BY_ANOTHER_POD
    }
}
