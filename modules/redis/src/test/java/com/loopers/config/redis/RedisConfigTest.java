package com.loopers.config.redis;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class RedisConfigTest {

    private RedisProperties properties(Duration commandTimeout) {
        return new RedisProperties(
                0,
                new RedisNodeInfo("localhost", 6379),
                List.of(new RedisNodeInfo("localhost", 6380)),
                commandTimeout
        );
    }

    @DisplayName("Redis 커넥션 팩토리를 생성할 때,")
    @Nested
    class CreateConnectionFactory {

        @DisplayName("command-timeout을 설정하면, 기본/master 커넥션 팩토리에 모두 적용된다.")
        @Test
        void applies_configuredCommandTimeout_toBothConnectionFactories() {
            RedisConfig config = new RedisConfig(properties(Duration.ofMillis(500)));

            LettuceConnectionFactory defaultFactory = config.defaultRedisConnectionFactory();
            LettuceConnectionFactory masterFactory = config.masterRedisConnectionFactory();

            assertAll(
                    () -> assertThat(defaultFactory.getClientConfiguration().getCommandTimeout())
                            .isEqualTo(Duration.ofMillis(500)),
                    () -> assertThat(masterFactory.getClientConfiguration().getCommandTimeout())
                            .isEqualTo(Duration.ofMillis(500))
            );
        }

        @DisplayName("command-timeout이 없으면, 기본값 3초가 적용된다.")
        @Test
        void applies_defaultThreeSeconds_whenCommandTimeoutIsMissing() {
            RedisConfig config = new RedisConfig(properties(null));

            LettuceConnectionFactory factory = config.defaultRedisConnectionFactory();

            assertThat(factory.getClientConfiguration().getCommandTimeout()).isEqualTo(Duration.ofSeconds(3));
        }
    }
}