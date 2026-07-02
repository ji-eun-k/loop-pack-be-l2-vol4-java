package com.loopers.interfaces.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.infrastructure.coupon.IssuedCouponJpaRepository;
import com.loopers.testcontainers.KafkaTestContainersConfig;
import com.loopers.testcontainers.MySqlTestContainersConfig;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import com.loopers.utils.RedisCleanUp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import({MySqlTestContainersConfig.class, RedisTestContainersConfig.class, KafkaTestContainersConfig.class})
class CouponIssueConsumerConcurrencyTest {

    private static final String COUPON_STOCK_KEY_PREFIX = "coupon:stock:";
    private static final Long COUPON_ID = 1L;
    private static final int STOCK = 50;
    private static final int TOTAL_REQUESTS = 100;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private IssuedCouponJpaRepository issuedCouponJpaRepository;

    @Autowired
    @Qualifier("redisTemplateMaster")
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @Autowired
    private RedisCleanUp redisCleanUp;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        // IssuedCouponJpaRepository.incrementIssuedCount()가 native query로 coupon 테이블을 참조하므로
        // streamer 모듈에 CouponEntity가 없어 ddl-auto로 생성되지 않는 coupon 테이블을 직접 생성
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS coupon (
                id           BIGINT PRIMARY KEY,
                issued_count INT NOT NULL DEFAULT 0
            )
        """);
        jdbcTemplate.update("INSERT IGNORE INTO coupon (id, issued_count) VALUES (?, 0)", COUPON_ID);

        String stockKey = COUPON_STOCK_KEY_PREFIX + COUPON_ID;
        redisTemplate.opsForValue().set(stockKey, String.valueOf(STOCK));
    }

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        jdbcTemplate.execute("TRUNCATE TABLE coupon");
        redisCleanUp.truncateAll();
    }

    @DisplayName("선착순 50개 쿠폰에 100명이 요청하면, 정확히 50건만 발급되고 재고는 0이 된다.")
    @Test
    void limitsCouponIssuance_whenConcurrentRequests() throws Exception {
        // Arrange: 100명 발급 요청 — couponId를 partition key로 사용해 동일 파티션에 배정
        ZonedDateTime expiredAt = ZonedDateTime.now().plusDays(30);
        for (long userId = 1; userId <= TOTAL_REQUESTS; userId++) {
            String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "couponId", COUPON_ID,
                "userId", userId,
                "expiredAt", expiredAt.toString()
            ));
            ProducerRecord<Object, Object> record = new ProducerRecord<>(
                "coupon-issue-requests", null, COUPON_ID.toString(), payload
            );
            kafkaTemplate.send(record).get();
        }

        // Act: 50건 발급 완료까지 대기
        await().atMost(30, SECONDS).until(() ->
            issuedCouponJpaRepository.count() >= STOCK
        );

        // 재고 소진 후 나머지 50건이 skip 처리될 시간 허용
        Thread.sleep(3_000);

        // Assert
        long issuedCount = issuedCouponJpaRepository.count();
        String remainingStock = redisTemplate.opsForValue().get(COUPON_STOCK_KEY_PREFIX + COUPON_ID);

        assertThat(issuedCount).isEqualTo(STOCK);
        assertThat(remainingStock).isEqualTo("0");
    }

    @DisplayName("동일 유저가 10번 중복 요청하는 와중에 다른 유저들도 동시에 요청하면, 총 50건만 발급되고 동일 유저는 1건만 발급된다.")
    @Test
    void limitsTotalAndDeduplicatesSameUser_whenMixedConcurrentRequests() throws Exception {
        // Arrange
        // - userId=1: 10번 중복 요청 (1건만 발급되어야 함)
        // - userId=2..100: 각 1번 요청 (재고 50개 한도 내에서 발급)
        // - 총 요청: 109건, 총 발급: 50건
        long duplicateUserId = 1L;
        int duplicateRequests = 10;
        int otherUserCount = 99;
        ZonedDateTime expiredAt = ZonedDateTime.now().plusDays(30);

        for (int i = 0; i < duplicateRequests; i++) {
            String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "couponId", COUPON_ID, "userId", duplicateUserId, "expiredAt", expiredAt.toString()
            ));
            kafkaTemplate.send(new ProducerRecord<>("coupon-issue-requests", null, COUPON_ID.toString(), payload)).get();
        }
        for (long userId = 2; userId <= otherUserCount + 1; userId++) {
            String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "couponId", COUPON_ID, "userId", userId, "expiredAt", expiredAt.toString()
            ));
            kafkaTemplate.send(new ProducerRecord<>("coupon-issue-requests", null, COUPON_ID.toString(), payload)).get();
        }

        // Act: 50건 발급 완료 후 나머지 메시지 처리 시간 허용
        await().atMost(30, SECONDS).until(() -> issuedCouponJpaRepository.count() >= STOCK);
        Thread.sleep(3_000);

        // Assert
        long totalIssued = issuedCouponJpaRepository.count();
        long issuedToSameUser = issuedCouponJpaRepository.countByCouponIdAndUserId(COUPON_ID, duplicateUserId);
        String remainingStock = redisTemplate.opsForValue().get(COUPON_STOCK_KEY_PREFIX + COUPON_ID);

        assertThat(totalIssued).isEqualTo(STOCK);
        assertThat(issuedToSameUser).isEqualTo(1);
        assertThat(remainingStock).isEqualTo("0");
    }

    @DisplayName("동일 유저가 동일 쿠폰을 10번 요청해도 1건만 발급된다.")
    @Test
    void issuesOnlyOnce_whenSameUserRequestsMultipleTimes() throws Exception {
        // Arrange: 동일 userId=1 로 10번 발급 요청
        long duplicateUserId = 1L;
        int duplicateRequests = 10;
        ZonedDateTime expiredAt = ZonedDateTime.now().plusDays(30);

        for (int i = 0; i < duplicateRequests; i++) {
            String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "couponId", COUPON_ID,
                "userId", duplicateUserId,
                "expiredAt", expiredAt.toString()
            ));
            ProducerRecord<Object, Object> record = new ProducerRecord<>(
                "coupon-issue-requests", null, COUPON_ID.toString(), payload
            );
            kafkaTemplate.send(record).get();
        }

        // Act: 1건 발급 완료 후 나머지 9건이 skip될 시간 허용
        await().atMost(30, SECONDS).until(() ->
            issuedCouponJpaRepository.count() >= 1
        );
        Thread.sleep(3_000);

        // Assert: 동일 유저 중복 요청이므로 1건만 발급, 재고는 49
        long issuedCount = issuedCouponJpaRepository.count();
        String remainingStock = redisTemplate.opsForValue().get(COUPON_STOCK_KEY_PREFIX + COUPON_ID);

        assertThat(issuedCount).isEqualTo(1);
        assertThat(remainingStock).isEqualTo(String.valueOf(STOCK - 1));
    }
}
