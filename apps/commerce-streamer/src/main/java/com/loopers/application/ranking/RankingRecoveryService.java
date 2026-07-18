package com.loopers.application.ranking;

import com.loopers.domain.ranking.CatalogRankingReplayEvent;
import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.infrastructure.catalog.CatalogRankingReplayRepository;
import com.loopers.infrastructure.ranking.RankingUpdater;
import com.loopers.support.ranking.RankingKeys;
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

/**
 * Redis 랭킹이 유실됐을 때 catalog_event_ledger(원장)를 다시 읽어 랭킹 점수를 재계산/복구한다.
 * 여러 파드가 동시에 스케줄을 돌리므로 Redis 분산락으로 중복 리플레이를 막는다.
 * 완료 마커가 있어도 실제 랭킹/처리 이력 키가 유실됐으면 원장에서 다시 복구한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RankingRecoveryService {

    private static final String LOCK_PREFIX = "ranking:recovery:lock:";
    private static final String COMPLETED_PREFIX = "ranking:recovery:completed:";
    // 락 소유자(owner) 값이 내 것일 때만 DEL하는 compare-and-delete.
    // GET 후 DEL을 따로 호출하면 그 사이 락이 만료→다른 파드가 재획득할 수 있어 Lua로 원자적으로 처리한다.
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
        if (isRecoveryCompleteAndDataIntact(date, completedKey)) {
            return RecoveryResult.alreadyCompleted(date);
        }

        // setIfAbsent(SET NX)로 원자적으로 락을 획득한다. owner는 이 실행 인스턴스를 식별해 락 오탈취를 막는다.
        String lockKey = LOCK_PREFIX + date;
        String owner = UUID.randomUUID().toString();
        Boolean locked = redisTemplate.opsForValue().setIfAbsent(lockKey, owner, properties.lockTtl());
        if (!Boolean.TRUE.equals(locked)) {
            return RecoveryResult.locked(date);
        }

        try {
            // 락을 얻는 동안 다른 파드가 복구했을 수 있으므로 실제 데이터까지 다시 확인한다.
            if (isRecoveryCompleteAndDataIntact(date, completedKey)) {
                return RecoveryResult.alreadyCompleted(date);
            }
            // 랭킹 또는 처리 이력 중 하나라도 유실됐다면 불완전한 상태 위에 덧대지 않고 함께 재구축한다.
            rankingUpdater.reset(date);
            RecoveryResult result = replay(date);
            redisTemplate.opsForValue().set(completedKey, "1", properties.markerTtl());
            return result;
        } finally {
            // 실패하더라도 락은 반드시 해제해 다음 스케줄에서 재시도할 수 있게 한다.
            redisTemplate.execute(RELEASE_LOCK_SCRIPT, List.of(lockKey), owner);
        }
    }

    private boolean isRecoveryCompleteAndDataIntact(LocalDate date, String completedKey) {
        if (!Boolean.TRUE.equals(redisTemplate.hasKey(completedKey))) {
            return false;
        }
        return Boolean.TRUE.equals(redisTemplate.hasKey(RankingKeys.daily(date)))
            && Boolean.TRUE.equals(redisTemplate.hasKey(RankingKeys.handled(date)));
    }

    private RecoveryResult replay(LocalDate date) {
        Instant from = date.atStartOfDay(clock.getZone()).toInstant();
        Instant to = date.plusDays(1).atStartOfDay(clock.getZone()).toInstant();
        long lastLedgerId = 0L;
        long scanned = 0L;
        long applied = 0L;

        // ledgerId 커서 기반 페이징: OFFSET 대신 마지막으로 읽은 id를 기준으로 다음 배치를 조회해
        // 대량 원장 테이블에서도 뒤로 갈수록 느려지지 않는다.
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

    /** 리플레이 실행 결과. scanned/applied는 COMPLETED 상태일 때만 의미 있는 값을 가진다. */
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
