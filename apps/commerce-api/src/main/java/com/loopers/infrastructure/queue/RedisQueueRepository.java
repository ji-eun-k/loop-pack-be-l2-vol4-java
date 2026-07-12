package com.loopers.infrastructure.queue;

import com.loopers.domain.queue.QueuePositionSnapshot;
import com.loopers.domain.queue.QueueRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class RedisQueueRepository implements QueueRepository {

    private static final String WAITING_KEY = "queue:waiting";

    @SuppressWarnings("unchecked")
    private static final RedisScript<List<Long>> ENTER_SCRIPT =
            RedisScript.of(QueueLuaScripts.ENTER, (Class<List<Long>>) (Class<?>) List.class);

    @SuppressWarnings("unchecked")
    private static final RedisScript<List<Long>> POSITION_SCRIPT =
            RedisScript.of(QueueLuaScripts.POSITION, (Class<List<Long>>) (Class<?>) List.class);

    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public List<Long> enter(Long userId) {
        return redisTemplate.execute(
                ENTER_SCRIPT,
                List.of(WAITING_KEY),
                userId.toString()
        );
    }

    @Override
    public List<Long> popOldest(int n) {
        Set<ZSetOperations.TypedTuple<String>> popped =
                redisTemplate.opsForZSet().popMin(WAITING_KEY, n);
        if (popped == null || popped.isEmpty()) {
            return List.of();
        }
        return popped.stream()
                .map(tuple -> Long.parseLong(tuple.getValue()))
                .toList();
    }

    @Override
    public Optional<QueuePositionSnapshot> findPositionSnapshot(Long userId) {
        List<Long> raw = redisTemplate.execute(
                POSITION_SCRIPT,
                List.of(WAITING_KEY),
                userId.toString()
        );
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        if (raw.size() == 1 && raw.get(0) == -1L) {
            return Optional.empty();
        }
        return Optional.of(new QueuePositionSnapshot(raw.get(0), raw.get(1)));
    }
}
