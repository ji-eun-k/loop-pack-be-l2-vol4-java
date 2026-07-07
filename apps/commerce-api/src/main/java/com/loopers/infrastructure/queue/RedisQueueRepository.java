package com.loopers.infrastructure.queue;

import com.loopers.domain.queue.QueueRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class RedisQueueRepository implements QueueRepository {

    private static final String WAITING_KEY = "queue:waiting";
    private static final String ACTIVE_KEY_PREFIX = "queue:active:";

    @SuppressWarnings("unchecked")
    private static final RedisScript<List<Long>> ENTER_SCRIPT =
            RedisScript.of(QueueLuaScripts.ENTER, (Class<List<Long>>) (Class<?>) List.class);

    @SuppressWarnings("unchecked")
    private static final RedisScript<List<Long>> POSITION_SCRIPT =
            RedisScript.of(QueueLuaScripts.POSITION, (Class<List<Long>>) (Class<?>) List.class);

    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public List<Long> enter(Long userId) {
        long score = System.currentTimeMillis();
        return redisTemplate.execute(
                ENTER_SCRIPT,
                List.of(WAITING_KEY),
                userId.toString(), String.valueOf(score)
        );
    }

    @Override
    public List<Long> getPosition(Long userId) {
        String activeKey = ACTIVE_KEY_PREFIX + userId;
        return redisTemplate.execute(
                POSITION_SCRIPT,
                List.of(WAITING_KEY, activeKey),
                userId.toString()
        );
    }
}
