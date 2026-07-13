package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingEventScore;
import com.loopers.support.ranking.RankingKeys;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 배치 단위로 집계된 상품별 점수 델타를 오늘 날짜 랭킹 ZSET에 반영한다.
 * TTL은 갱신 때마다 2일로 리셋 — 키가 날짜별이므로 "그 날짜의 마지막 쓰기 + 2일"에 만료된다.
 */
@RequiredArgsConstructor
@Component
public class RankingUpdater {

    private static final Duration RANKING_TTL = Duration.ofDays(2);
    private static final double CARRY_OVER_WEIGHT = 0.1;
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

    public void applyDeltas(Map<Long, Double> deltas) {
        applyDeltas(LocalDate.now(clock), deltas);
    }

    public void applyDeltas(LocalDate date, Map<Long, Double> deltas) {
        if (deltas.isEmpty()) {
            return;
        }
        String key = RankingKeys.daily(date);
        deltas.forEach((productId, delta) ->
            redisTemplate.opsForZSet().incrementScore(key, productId.toString(), delta)
        );
        redisTemplate.expire(key, RANKING_TTL);
    }

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
     * 자정 전환 시 콜드 스타트 방지: 오늘 점수의 0.1배로 내일 키를 미리 생성한다.
     * ZUNIONSTORE는 대상 키를 덮어쓰므로 재실행해도 오늘 점수 기준으로 재계산된다 (멱등).
     * 오늘 키가 없으면 결과가 빈 ZSET이라 Redis가 대상 키를 만들지 않는다.
     */
    public void carryOverToNextDay() {
        LocalDate today = LocalDate.now(clock);
        String todayKey = RankingKeys.daily(today);
        String tomorrowKey = RankingKeys.daily(today.plusDays(1));
        Long carried = redisTemplate.opsForZSet().unionAndStore(
            todayKey, List.of(), tomorrowKey, Aggregate.SUM, Weights.of(CARRY_OVER_WEIGHT)
        );
        if (carried != null && carried > 0) {
            redisTemplate.expire(tomorrowKey, RANKING_TTL);
        }
    }
}
