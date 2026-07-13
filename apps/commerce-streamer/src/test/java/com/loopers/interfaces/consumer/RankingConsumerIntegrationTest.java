package com.loopers.interfaces.consumer;

import com.loopers.infrastructure.catalog.EventHandledJpaRepository;
import com.loopers.support.ranking.RankingKeys;
import com.loopers.testcontainers.KafkaTestContainersConfig;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import com.loopers.utils.RedisCleanUp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.LocalDate;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({MySqlTestContainersConfig.class, RedisTestContainersConfig.class, KafkaTestContainersConfig.class})
class RankingConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private EventHandledJpaRepository eventHandledJpaRepository;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private Clock clock;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @Autowired
    private RedisCleanUp redisCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        redisCleanUp.truncateAll();
    }

    private String todayKey() {
        return RankingKeys.daily(LocalDate.now(clock));
    }

    private Double score(long productId) {
        return redisTemplate.opsForZSet().score(todayKey(), String.valueOf(productId));
    }

    @DisplayName("catalog-events-v1 소비 시 랭킹 ZSET에")
    @Nested
    class CatalogEvents {

        @DisplayName("OrderItemSoldEvent의 productAmountMap 기준 주문 점수가 반영된다.")
        @Test
        void appliesOrderScore_whenOrderItemSoldEventReceived() throws Exception {
            String eventId = UUID.randomUUID().toString();

            sendWithHeaders("OrderItemSoldEvent", eventId,
                "{\"orderId\":1,\"productQtyMap\":{\"7001\":2},\"productAmountMap\":{\"7001\":24000}}");

            await().atMost(30, SECONDS).until(() -> score(7001L) != null);

            assertThat(score(7001L)).isNotNull();
            assertThat(score(7001L)).isCloseTo(0.6 * Math.log10(1 + 24_000), within(1e-6));
        }

        @DisplayName("동일한 eventId가 두 번 오면 점수는 한 번만 적립된다.")
        @Test
        void doesNotDoubleApplyScore_whenDuplicateEventId() throws Exception {
            String eventId = UUID.randomUUID().toString();
            String payload = "{\"productId\":7002}";

            sendWithHeaders("ProductLikedEvent", eventId, payload);
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventId));

            sendWithHeaders("ProductLikedEvent", eventId, payload);
            Thread.sleep(3_000);

            assertThat(score(7002L)).isCloseTo(0.2, within(1e-9));
        }

        @DisplayName("좋아요 후 취소하면 점수가 차감되어 0으로 돌아간다.")
        @Test
        void decreasesScore_whenUnlikedAfterLiked() throws Exception {
            String likeId = UUID.randomUUID().toString();
            String unlikeId = UUID.randomUUID().toString();

            sendWithHeaders("ProductLikedEvent", likeId, "{\"productId\":7003}");
            await().atMost(30, SECONDS).until(() -> score(7003L) != null);

            sendWithHeaders("ProductUnlikedEvent", unlikeId, "{\"productId\":7003}");
            await().atMost(30, SECONDS).untilAsserted(() ->
                assertThat(score(7003L)).isCloseTo(0.0, within(1e-9))
            );

            assertThat(score(7003L)).isCloseTo(0.0, within(1e-9));
        }
    }

    @DisplayName("catalog-view-events-v1 소비 시 랭킹 ZSET에")
    @Nested
    class ViewEvents {

        @DisplayName("조회 이벤트당 view 가중치 점수가 반영된다.")
        @Test
        void appliesViewScore_whenProductViewedEventReceived() throws Exception {
            kafkaTemplate.send("catalog-view-events-v1", null, "{\"productId\":7004}").get();

            await().atMost(30, SECONDS).until(() -> score(7004L) != null);

            assertThat(score(7004L)).isCloseTo(0.1, within(1e-9));
        }
    }

    private void sendWithHeaders(String eventType, String eventId, String payload) throws Exception {
        ProducerRecord<Object, Object> record = new ProducerRecord<>("catalog-events-v1", "1", payload);
        record.headers().add("X-Event-Type", eventType.getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Event-Id", eventId.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get();
    }
}
