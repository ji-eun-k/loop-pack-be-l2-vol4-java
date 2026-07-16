package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingItem;
import com.loopers.domain.ranking.RankingRepository;
import com.loopers.support.ranking.RankingKeys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "ranking.repository", havingValue = "redis", matchIfMissing = true)
public class RedisRankingRepository implements RankingRepository {

    private final RedisTemplate<String, String> redisTemplate;
    private final RankingTopSnapshotRepository snapshotRepository;

    @Override
    public List<RankingItem> findPage(LocalDate date, int page, int size) {
        try {
            long start = (long) page * size;
            long end = start + size - 1;
            Set<ZSetOperations.TypedTuple<String>> tuples =
                redisTemplate.opsForZSet().reverseRangeWithScores(RankingKeys.daily(date), start, end);
            if (tuples == null || tuples.isEmpty()) {
                return List.of();
            }
            return tuples.stream()
                .filter(tuple -> tuple.getValue() != null && tuple.getScore() != null)
                .map(tuple -> new RankingItem(Long.parseLong(tuple.getValue()), tuple.getScore()))
                .toList();
        } catch (RedisConnectionFailureException | RedisSystemException | QueryTimeoutException exception) {
            log.warn("[RANKING] Redis 목록 조회 실패 — Top-N snapshot fallback, date={}", date, exception);
            return snapshotRepository.findPage(date, page, size);
        }
    }

    @Override
    public Optional<Long> findRank(LocalDate date, Long productId) {
        try {
            Long zeroBasedRank = redisTemplate.opsForZSet().reverseRank(RankingKeys.daily(date), productId.toString());
            return Optional.ofNullable(zeroBasedRank).map(rank -> rank + 1);
        } catch (RedisConnectionFailureException | RedisSystemException | QueryTimeoutException exception) {
            log.warn("[RANKING] Redis 단건 순위 조회 실패 — Top-N snapshot fallback, date={}, productId={}",
                date, productId, exception);
            return snapshotRepository.findRank(date, productId);
        }
    }

    @Override
    public long countTotal(LocalDate date) {
        try {
            Long size = redisTemplate.opsForZSet().size(RankingKeys.daily(date));
            return size == null ? 0L : size;
        } catch (RedisConnectionFailureException | RedisSystemException | QueryTimeoutException exception) {
            log.warn("[RANKING] Redis 랭킹 수 조회 실패 — Top-N snapshot fallback, date={}", date, exception);
            return snapshotRepository.count(date);
        }
    }
}
