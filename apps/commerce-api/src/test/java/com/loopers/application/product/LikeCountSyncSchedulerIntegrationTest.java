package com.loopers.application.product;

import com.loopers.infrastructure.brand.BrandEntity;
import com.loopers.infrastructure.brand.BrandJpaRepository;
import com.loopers.infrastructure.product.ProductEntity;
import com.loopers.infrastructure.product.ProductJpaRepository;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(RedisTestContainersConfig.class)
class LikeCountSyncSchedulerIntegrationTest {

    @Autowired
    private LikeCountSyncScheduler likeCountSyncScheduler;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private BrandJpaRepository brandJpaRepository;

    @Autowired
    private ProductJpaRepository productJpaRepository;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        redisTemplate.delete(redisTemplate.keys("product:like:pending:*"));
    }

    @DisplayName("productLikeSync()")
    @Nested
    class Sync {

        @DisplayName("Redis에 pending이 있으면, MySQL likeCount에 반영하고 Redis 키를 삭제한다.")
        @Test
        void syncsPendingToMysqlAndClearsRedisKey() {
            // Arrange
            BrandEntity brand = brandJpaRepository.save(new BrandEntity("브랜드", "설명"));
            ProductEntity product = productJpaRepository.save(
                new ProductEntity(brand.getId(), "청바지", BigDecimal.valueOf(50000)));
            redisTemplate.opsForValue().set("product:like:pending:" + product.getId(), "5");

            // Act
            likeCountSyncScheduler.productLikeSync();

            // Assert
            ProductEntity result = productJpaRepository.findById(product.getId()).orElseThrow();
            assertThat(result.getLikeCount()).isEqualTo(5);
            assertThat(redisTemplate.hasKey("product:like:pending:" + product.getId())).isFalse();
        }

        @DisplayName("Redis pending이 음수이면, MySQL likeCount를 감소시킨다.")
        @Test
        void decreasesLikeCount_whenPendingIsNegative() {
            // Arrange
            BrandEntity brand = brandJpaRepository.save(new BrandEntity("브랜드", "설명"));
            ProductEntity product = productJpaRepository.save(
                new ProductEntity(brand.getId(), "청바지", BigDecimal.valueOf(50000)));
            productJpaRepository.incrementLikeCount(product.getId());
            productJpaRepository.incrementLikeCount(product.getId());
            redisTemplate.opsForValue().set("product:like:pending:" + product.getId(), "-1");

            // Act
            likeCountSyncScheduler.productLikeSync();

            // Assert
            ProductEntity result = productJpaRepository.findById(product.getId()).orElseThrow();
            assertThat(result.getLikeCount()).isEqualTo(1);
        }

        @DisplayName("pending 적용 후 likeCount가 0 미만이 되면, 0으로 유지한다.")
        @Test
        void likeCountDoesNotGoBelowZero_whenPendingExceedsCurrentCount() {
            // Arrange
            BrandEntity brand = brandJpaRepository.save(new BrandEntity("브랜드", "설명"));
            ProductEntity product = productJpaRepository.save(
                new ProductEntity(brand.getId(), "청바지", BigDecimal.valueOf(50000)));
            redisTemplate.opsForValue().set("product:like:pending:" + product.getId(), "-5");

            // Act
            likeCountSyncScheduler.productLikeSync();

            // Assert
            ProductEntity result = productJpaRepository.findById(product.getId()).orElseThrow();
            assertThat(result.getLikeCount()).isEqualTo(0);
        }

        @DisplayName("Redis pending이 없으면, MySQL likeCount는 변하지 않는다.")
        @Test
        void doesNotChangeLikeCount_whenNoPendingInRedis() {
            // Arrange
            BrandEntity brand = brandJpaRepository.save(new BrandEntity("브랜드", "설명"));
            ProductEntity product = productJpaRepository.save(
                new ProductEntity(brand.getId(), "청바지", BigDecimal.valueOf(50000)));

            // Act
            likeCountSyncScheduler.productLikeSync();

            // Assert
            ProductEntity result = productJpaRepository.findById(product.getId()).orElseThrow();
            assertThat(result.getLikeCount()).isEqualTo(0);
        }

        @DisplayName("여러 상품의 pending을 한 번에 MySQL에 반영한다.")
        @Test
        void syncsMultipleProductsAtOnce() {
            // Arrange
            BrandEntity brand = brandJpaRepository.save(new BrandEntity("브랜드", "설명"));
            ProductEntity productA = productJpaRepository.save(
                new ProductEntity(brand.getId(), "청바지", BigDecimal.valueOf(50000)));
            ProductEntity productB = productJpaRepository.save(
                new ProductEntity(brand.getId(), "티셔츠", BigDecimal.valueOf(30000)));
            redisTemplate.opsForValue().set("product:like:pending:" + productA.getId(), "3");
            redisTemplate.opsForValue().set("product:like:pending:" + productB.getId(), "7");

            // Act
            likeCountSyncScheduler.productLikeSync();

            // Assert
            assertThat(productJpaRepository.findById(productA.getId()).orElseThrow().getLikeCount()).isEqualTo(3);
            assertThat(productJpaRepository.findById(productB.getId()).orElseThrow().getLikeCount()).isEqualTo(7);
        }
    }
}