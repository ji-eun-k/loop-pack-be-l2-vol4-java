package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.support.ranking.RankingKeys;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 배치 단위로 집계된 상품별 점수 델타를 오늘 날짜 랭킹 ZSET에 반영한다.
 * TTL은 갱신 때마다 2일로 리셋 — 키가 날짜별이므로 "그 날짜의 마지막 쓰기 + 2일"에 만료된다.
 */
@RequiredArgsConstructor
@Component
public class RankingUpdater {

    private static final Duration RANKING_TTL = Duration.ofDays(2);
    private static final Duration CARRY_OVER_LOCK_TTL = Duration.ofDays(1);
    // KEYS[1]=오늘 ZSET, KEYS[2]=내일 ZSET, KEYS[3]=중복 실행 방지 락.
    // 반환값: 0 이상=carry-over한 항목 수, -1=다른 파드가 이미 락 선점, -2=내일 키가 이미 존재해 skip.
    private static final DefaultRedisScript<Long> CARRY_OVER_SCRIPT = new DefaultRedisScript<>("""
        if redis.call('EXISTS', KEYS[2]) == 1 then
            return -2
        end
        if not redis.call('SET', KEYS[3], ARGV[1], 'NX', 'EX', ARGV[2]) then
            return -1
        end
        local entries = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
        if #entries == 0 then
            redis.call('DEL', KEYS[3])
            return 0
        end
        for i = 1, #entries, 2 do
            redis.call('ZADD', KEYS[2], tonumber(entries[i + 1]) * tonumber(ARGV[3]), entries[i])
        end
        redis.call('EXPIRE', KEYS[2], ARGV[4])
        return #entries / 2
        """, Long.class);
    // KEYS[1]=날짜별 랭킹 ZSET, KEYS[2]=처리된 eventId를 담는 SET(멱등 판단용).
    // ARGV는 [ttl, eventId, itemCount, (productId, delta) * itemCount, ...] 형태로 이벤트를 평탄화해서 넘긴다.
    // SADD가 0을 반환하면(이미 처리된 eventId) 해당 이벤트의 항목만 건너뛰고 다음 이벤트로 이동한다.
    private static final DefaultRedisScript<Long> APPLY_EVENTS_SCRIPT = new DefaultRedisScript<>("""
        local ttl = tonumber(ARGV[1])
        local index = 2
        local applied = 0
        while index <= #ARGV do
            local eventId = ARGV[index]
            local itemCount = tonumber(ARGV[index + 1])
            index = index + 2
            if redis.call('SADD', KEYS[2], eventId) == 1 then
                for i = 1, itemCount do
                    redis.call('ZINCRBY', KEYS[1], ARGV[index + 1], ARGV[index])
                    index = index + 2
                end
                applied = applied + 1
            else
                index = index + (itemCount * 2)
            end
        end
        if applied > 0 then
            redis.call('EXPIRE', KEYS[1], ttl)
            redis.call('EXPIRE', KEYS[2], ttl)
        end
        return applied
        """, Long.class);

    private final RedisTemplate<String, String> redisTemplate;
    private final Clock clock;
    private final RankingCarryOverProperties carryOverProperties;

    /**
     * 이벤트 ID 확인과 점수 증가를 하나의 Lua script로 실행한다.
     * 동일 Kafka batch가 재처리되어도 각 이벤트 점수는 한 번만 증가한다.
     */
    public long applyEvents(LocalDate date, List<RankingEventScore> events) {
        List<RankingEventScore> scoredEvents = events.stream()
            .filter(event -> !event.deltas().isEmpty())
            .toList();
        if (scoredEvents.isEmpty()) {
            return 0L;
        }

        List<String> args = new ArrayList<>();
        args.add(String.valueOf(RANKING_TTL.toSeconds()));
        for (RankingEventScore event : scoredEvents) {
            args.add(event.eventId());
            args.add(String.valueOf(event.deltas().size()));
            event.deltas().forEach((productId, delta) -> {
                args.add(productId.toString());
                args.add(Double.toString(delta));
            });
        }

        Long applied = redisTemplate.execute(
            APPLY_EVENTS_SCRIPT,
            List.of(RankingKeys.daily(date), RankingKeys.handled(date)),
            args.toArray()
        );
        return applied == null ? 0L : applied;
    }

    /**
     * 원장 리플레이 전에 랭킹과 처리 이력을 함께 비운다.
     * 둘 중 하나만 유실된 상태에서 리플레이하면 점수가 누락되거나 중복될 수 있으므로
     * 동일 날짜의 두 키를 하나의 Redis 명령으로 삭제한다.
     */
    public void reset(LocalDate date) {
        redisTemplate.delete(List.of(RankingKeys.daily(date), RankingKeys.handled(date)));
    }

    /**
     * 자정 전환 시 콜드 스타트 방지: 오늘 점수의 0.1배로 내일 키를 미리 생성한다.
     * ZUNIONSTORE는 대상 키를 덮어쓰므로 재실행해도 오늘 점수 기준으로 재계산된다 (멱등).
     * 오늘 키가 없으면 결과가 빈 ZSET이라 Redis가 대상 키를 만들지 않는다.
     */
    public long carryOverToNextDay() {
        LocalDate today = LocalDate.now(clock);
        String todayKey = RankingKeys.daily(today);
        String tomorrowKey = RankingKeys.daily(today.plusDays(1));
        String lockKey = "ranking:carry-over:" + today;
        Long carried = redisTemplate.execute(
            CARRY_OVER_SCRIPT,
            List.of(todayKey, tomorrowKey, lockKey),
            java.util.UUID.randomUUID().toString(),
            Long.toString(CARRY_OVER_LOCK_TTL.toSeconds()),
            Double.toString(carryOverProperties.weight()),
            Long.toString(RANKING_TTL.toSeconds())
        );
        return carried == null ? 0L : carried;
    }
}
