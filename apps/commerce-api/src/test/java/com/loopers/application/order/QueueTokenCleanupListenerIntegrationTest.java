package com.loopers.application.order;

import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@SpringBootTest
@Import(RedisTestContainersConfig.class)
class QueueTokenCleanupListenerIntegrationTest {

    @Autowired private ApplicationEventPublisher eventPublisher;
    @Autowired private EntryTokenRepository entryTokenRepository;
    @Autowired private RedisCleanUp redisCleanUp;

    @AfterEach
    void tearDown() {
        redisCleanUp.truncateAll();
    }

    @DisplayName("OrderCompletedEvent가 발행될 때,")
    @Nested
    class Handle {

        @DisplayName("해당 유저의 입장 토큰이 삭제된다.")
        @Test
        void deletesToken_whenOrderCompleted() {
            // arrange
            Long userId = 1L;
            entryTokenRepository.save(userId, "test-token", 300);

            // act
            eventPublisher.publishEvent(new OrderCompletedEvent(userId));

            // assert
            assertThat(entryTokenRepository.find(userId)).isEmpty();
        }

        @DisplayName("토큰이 없는 유저여도 예외가 발생하지 않는다.")
        @Test
        void doesNotThrow_whenTokenDoesNotExist() {
            // act & assert
            assertThatNoException().isThrownBy(() ->
                    eventPublisher.publishEvent(new OrderCompletedEvent(999L))
            );
        }
    }
}
