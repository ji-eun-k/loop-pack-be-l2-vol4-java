package com.loopers.application.product;

import com.loopers.domain.product.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@RequiredArgsConstructor
@Component
public class LikeCountSyncScheduler {

    private static final String PENDING_KEY_PATTERN = "product:like:pending:*";
    private static final String PENDING_KEY_PREFIX = "product:like:pending:";

    private final RedisTemplate<String, String> redisTemplate;
    private final ProductRepository productRepository;

    @Scheduled(fixedDelay = 300_000)
    public void sync() {
        Set<String> keys = redisTemplate.keys(PENDING_KEY_PATTERN);
        if (keys == null || keys.isEmpty()) {
            return;
        }

        for (String key : keys) {
            String value = redisTemplate.opsForValue().getAndDelete(key);
            if (value == null) {
                continue;
            }
            long pending = Long.parseLong(value);
            if (pending == 0) {
                continue;
            }
            Long productId = Long.parseLong(key.replace(PENDING_KEY_PREFIX, ""));
            applyToDatabase(productId, pending);
        }
    }

    private void applyToDatabase(Long productId, long pending) {
        productRepository.adjustLikeCount(productId, pending);
    }
}