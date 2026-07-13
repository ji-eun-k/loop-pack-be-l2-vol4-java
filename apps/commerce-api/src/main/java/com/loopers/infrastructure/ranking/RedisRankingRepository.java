package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingItem;
import com.loopers.domain.ranking.RankingRepository;
import com.loopers.support.ranking.RankingKeys;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class RedisRankingRepository implements RankingRepository {

    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public List<RankingItem> findPage(LocalDate date, int page, int size) {
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
    }

    @Override
    public Optional<Long> findRank(LocalDate date, Long productId) {
        Long zeroBasedRank = redisTemplate.opsForZSet().reverseRank(RankingKeys.daily(date), productId.toString());
        return Optional.ofNullable(zeroBasedRank).map(rank -> rank + 1);
    }

    @Override
    public long countTotal(LocalDate date) {
        Long size = redisTemplate.opsForZSet().size(RankingKeys.daily(date));
        return size == null ? 0L : size;
    }
}
