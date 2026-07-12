package com.loopers.infrastructure.queue;

import com.loopers.domain.queue.EntryTokenRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Repository
@RequiredArgsConstructor
public class RedisEntryTokenRepository implements EntryTokenRepository {

    private static final String KEY_PREFIX = "queue:active:";

    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public void save(Long userId, String token, long ttlSeconds) {
        redisTemplate.opsForValue().set(key(userId), token, ttlSeconds, TimeUnit.SECONDS);
    }

    @Override
    public Optional<String> find(Long userId) {
        return Optional.ofNullable(redisTemplate.opsForValue().get(key(userId)));
    }

    @Override
    public void delete(Long userId) {
        redisTemplate.delete(key(userId));
    }

    private String key(Long userId) {
        return KEY_PREFIX + userId;
    }
}
