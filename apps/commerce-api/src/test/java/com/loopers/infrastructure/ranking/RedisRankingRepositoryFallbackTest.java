package com.loopers.infrastructure.ranking;

import com.loopers.domain.ranking.RankingItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class RedisRankingRepositoryFallbackTest {

    private static final LocalDate DATE = LocalDate.of(2026, 7, 16);

    @Mock private RedisTemplate<String, String> redisTemplate;
    @Mock private ZSetOperations<String, String> zSetOperations;
    @Mock private RankingTopSnapshotRepository snapshotRepository;

    private RedisRankingRepository repository;

    @BeforeEach
    void setUp() {
        given(redisTemplate.opsForZSet()).willReturn(zSetOperations);
        repository = new RedisRankingRepository(redisTemplate, snapshotRepository);
    }

    @Test
    void fallsBackToSnapshotWhenRedisPageQueryFails() {
        List<RankingItem> snapshot = List.of(new RankingItem(1L, 10.0));
        given(zSetOperations.reverseRangeWithScores("ranking:all:20260716", 0, 19))
            .willThrow(new RedisConnectionFailureException("redis unavailable"));
        given(snapshotRepository.findPage(DATE, 0, 20)).willReturn(snapshot);

        assertThat(repository.findPage(DATE, 0, 20)).isEqualTo(snapshot);
    }

    @Test
    void fallsBackToSnapshotWhenRedisRankQueryFails() {
        given(zSetOperations.reverseRank("ranking:all:20260716", "1"))
            .willThrow(new RedisConnectionFailureException("redis unavailable"));
        given(snapshotRepository.findRank(DATE, 1L)).willReturn(Optional.of(2L));

        assertThat(repository.findRank(DATE, 1L)).contains(2L);
    }

    @Test
    void fallsBackToSnapshotCountWhenRedisCountQueryFails() {
        given(zSetOperations.size("ranking:all:20260716"))
            .willThrow(new RedisConnectionFailureException("redis unavailable"));
        given(snapshotRepository.count(DATE)).willReturn(100L);

        assertThat(repository.countTotal(DATE)).isEqualTo(100L);
    }

    @Test
    void fallsBackToSnapshotWhenRedisPageQueryTimesOut() {
        List<RankingItem> snapshot = List.of(new RankingItem(1L, 10.0));
        given(zSetOperations.reverseRangeWithScores("ranking:all:20260716", 0, 19))
            .willThrow(new QueryTimeoutException("redis command timed out"));
        given(snapshotRepository.findPage(DATE, 0, 20)).willReturn(snapshot);

        assertThat(repository.findPage(DATE, 0, 20)).isEqualTo(snapshot);
    }

    @Test
    void fallsBackToSnapshotWhenRedisRankQueryTimesOut() {
        given(zSetOperations.reverseRank("ranking:all:20260716", "1"))
            .willThrow(new QueryTimeoutException("redis command timed out"));
        given(snapshotRepository.findRank(DATE, 1L)).willReturn(Optional.of(2L));

        assertThat(repository.findRank(DATE, 1L)).contains(2L);
    }

    @Test
    void fallsBackToSnapshotCountWhenRedisCountQueryTimesOut() {
        given(zSetOperations.size("ranking:all:20260716"))
            .willThrow(new QueryTimeoutException("redis command timed out"));
        given(snapshotRepository.count(DATE)).willReturn(100L);

        assertThat(repository.countTotal(DATE)).isEqualTo(100L);
    }
}
