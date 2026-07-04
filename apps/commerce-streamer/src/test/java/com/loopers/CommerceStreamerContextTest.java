package com.loopers;

import com.loopers.testcontainers.KafkaTestContainersConfig;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import({MySqlTestContainersConfig.class, RedisTestContainersConfig.class, KafkaTestContainersConfig.class})
class CommerceStreamerContextTest {

    @Test
    void contextLoads() {
    }
}
