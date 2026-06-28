package com.loopers.infrastructure.product;

import com.loopers.domain.product.ProductLikeCountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

@RequiredArgsConstructor
@Repository
public class ProductLikeCountRepositoryImpl implements ProductLikeCountRepository {

    private static final String PENDING_KEY_PREFIX = "product:like:pending:";

    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public void increment(Long productId) {
        redisTemplate.opsForValue().increment(PENDING_KEY_PREFIX + productId);
    }

    @Override
    public void decrement(Long productId) {
        redisTemplate.opsForValue().decrement(PENDING_KEY_PREFIX + productId);
    }
}