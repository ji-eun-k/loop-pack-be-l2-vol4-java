package com.loopers.interfaces.consumer;

import com.loopers.infrastructure.catalog.EventHandledJpaRepository;
import com.loopers.infrastructure.catalog.ProductMetricsJpaRepository;
import com.loopers.testcontainers.KafkaTestContainersConfig;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({MySqlTestContainersConfig.class, RedisTestContainersConfig.class, KafkaTestContainersConfig.class})
class CatalogMetricsConsumerIntegrationTest {

    @TestConfiguration
    static class TopicConfig {
        @Bean
        public NewTopic catalogEventsTopic() {
            return new NewTopic("catalog-events-v1", 1, (short) 1);
        }

        @Bean
        public NewTopic catalogViewEventsTopic() {
            return new NewTopic("catalog-view-events-v1", 1, (short) 1);
        }
    }

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private EventHandledJpaRepository eventHandledJpaRepository;

    @Autowired
    private ProductMetricsJpaRepository productMetricsJpaRepository;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @DisplayName("CatalogMetricsConsumer — catalog-events-v1")
    @Nested
    class CatalogEvents {

        @DisplayName("OrderItemSoldEvent 수신 시 product_metrics에 order_count가 반영된다.")
        @Test
        void updatesOrderCount_whenOrderItemSoldEventReceived() throws Exception {
            String eventId = UUID.randomUUID().toString();

            sendWithHeaders("catalog-events-v1", "1", "OrderItemSoldEvent", eventId,
                "{\"orderId\":1,\"productQtyMap\":{\"1001\":2,\"1002\":3}}");

            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventId));

            assertThat(productMetricsJpaRepository.findById(1001L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getOrderCount()).isEqualTo(2));
            assertThat(productMetricsJpaRepository.findById(1002L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getOrderCount()).isEqualTo(3));
        }

        @DisplayName("동일한 eventId로 OrderItemSoldEvent가 두 번 오면 두 번째는 무시된다.")
        @Test
        void ignoresDuplicateOrderItemSoldEvent() throws Exception {
            String eventId = UUID.randomUUID().toString();
            String payload = "{\"orderId\":1,\"productQtyMap\":{\"2001\":2}}";

            sendWithHeaders("catalog-events-v1", "1", "OrderItemSoldEvent", eventId, payload);
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventId));

            sendWithHeaders("catalog-events-v1", "1", "OrderItemSoldEvent", eventId, payload);
            Thread.sleep(3_000);

            assertThat(productMetricsJpaRepository.findById(2001L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getOrderCount()).isEqualTo(2));
        }

        @DisplayName("ProductLikedEvent 수신 시 like_count가 1 증가한다.")
        @Test
        void incrementsLikeCount_whenProductLikedEventReceived() throws Exception {
            String eventId = UUID.randomUUID().toString();

            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", eventId, "{\"productId\":3001}");

            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventId));

            assertThat(productMetricsJpaRepository.findById(3001L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(1));
        }

        @DisplayName("동일한 eventId로 ProductLikedEvent가 두 번 오면 두 번째는 무시된다.")
        @Test
        void ignoresDuplicateProductLikedEvent() throws Exception {
            String eventId = UUID.randomUUID().toString();
            String payload = "{\"productId\":4001}";

            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", eventId, payload);
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventId));

            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", eventId, payload);
            Thread.sleep(3_000);

            assertThat(productMetricsJpaRepository.findById(4001L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(1));
        }
    }

    @DisplayName("두 유저의 좋아요/취소 흐름")
    @Nested
    class LikeUnlikeFlow {

        @DisplayName("유저 A·B 순으로 좋아요하면 like_count가 2가 된다.")
        @Test
        void likesFromTwoUsers() throws Exception {
            String eventIdA = UUID.randomUUID().toString();
            String eventIdB = UUID.randomUUID().toString();

            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", eventIdA, "{\"productId\":6001}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventIdA));

            sendWithHeaders("catalog-events-v1", "2", "ProductLikedEvent", eventIdB, "{\"productId\":6001}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventIdB));

            assertThat(productMetricsJpaRepository.findById(6001L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(2));
        }

        @DisplayName("유저 A·B 좋아요 후 각각 취소하면 like_count가 0이 된다.")
        @Test
        void likeAndUnlikeByTwoUsers() throws Exception {
            String likeA = UUID.randomUUID().toString();
            String likeB = UUID.randomUUID().toString();
            String unlikeA = UUID.randomUUID().toString();
            String unlikeB = UUID.randomUUID().toString();

            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", likeA, "{\"productId\":6002}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(likeA));

            sendWithHeaders("catalog-events-v1", "2", "ProductLikedEvent", likeB, "{\"productId\":6002}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(likeB));

            assertThat(productMetricsJpaRepository.findById(6002L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(2));

            sendWithHeaders("catalog-events-v1", "1", "ProductUnlikedEvent", unlikeA, "{\"productId\":6002}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(unlikeA));

            assertThat(productMetricsJpaRepository.findById(6002L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(1));

            sendWithHeaders("catalog-events-v1", "2", "ProductUnlikedEvent", unlikeB, "{\"productId\":6002}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(unlikeB));

            assertThat(productMetricsJpaRepository.findById(6002L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(0));
        }

        @DisplayName("유저 A의 좋아요 이벤트가 중복 수신돼도 like_count는 1이다.")
        @Test
        void duplicateLikeByUserA_doesNotDoubleCount() throws Exception {
            String eventIdA = UUID.randomUUID().toString();
            String payload = "{\"productId\":6003}";

            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", eventIdA, payload);
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(eventIdA));

            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", eventIdA, payload);
            Thread.sleep(3_000);

            assertThat(productMetricsJpaRepository.findById(6003L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(1));
        }

        @DisplayName("유저 A의 취소 이벤트가 중복 수신돼도 like_count는 유저 B의 좋아요 1개를 유지한다.")
        @Test
        void duplicateUnlikeByUserA_doesNotDecrementAgain() throws Exception {
            String likeA = UUID.randomUUID().toString();
            String likeB = UUID.randomUUID().toString();
            String unlikeA = UUID.randomUUID().toString();

            // 유저 A, B 모두 좋아요 → like_count = 2
            sendWithHeaders("catalog-events-v1", "1", "ProductLikedEvent", likeA, "{\"productId\":6004}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(likeA));

            sendWithHeaders("catalog-events-v1", "2", "ProductLikedEvent", likeB, "{\"productId\":6004}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(likeB));

            // 유저 A 취소 → like_count = 1
            sendWithHeaders("catalog-events-v1", "1", "ProductUnlikedEvent", unlikeA, "{\"productId\":6004}");
            await().atMost(30, SECONDS).until(() -> eventHandledJpaRepository.existsByEventId(unlikeA));

            // 유저 A 취소 중복 → 멱등성으로 skip → like_count = 1 유지 (실패 시 0이 됨)
            sendWithHeaders("catalog-events-v1", "1", "ProductUnlikedEvent", unlikeA, "{\"productId\":6004}");
            Thread.sleep(3_000);

            assertThat(productMetricsJpaRepository.findById(6004L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getLikeCount()).isEqualTo(1));
        }
    }

    @DisplayName("CatalogViewConsumer — catalog-view-events-v1")
    @Nested
    class ViewEvents {

        @DisplayName("ProductViewedEvent 수신 시 product_metrics에 view_count가 1 증가한다.")
        @Test
        void incrementsViewCount_whenProductViewedEventReceived() throws Exception {
            kafkaTemplate.send("catalog-view-events-v1", null, "{\"productId\":5001}").get();

            await().atMost(30, SECONDS)
                .until(() -> productMetricsJpaRepository.findById(5001L)
                    .map(m -> m.getViewCount() >= 1)
                    .orElse(false));

            assertThat(productMetricsJpaRepository.findById(5001L))
                .isPresent().hasValueSatisfying(m -> assertThat(m.getViewCount()).isEqualTo(1));
        }
    }

    private void sendWithHeaders(String topic, String key, String eventType, String eventId, String payload) throws Exception {
        ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, payload);
        record.headers().add("X-Event-Type", eventType.getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Event-Id", eventId.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get();
    }
}
