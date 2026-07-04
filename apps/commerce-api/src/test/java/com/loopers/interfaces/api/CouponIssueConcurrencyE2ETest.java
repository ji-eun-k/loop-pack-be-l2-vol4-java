package com.loopers.interfaces.api;

import com.loopers.domain.coupon.CouponType;
import com.loopers.infrastructure.coupon.CouponEntity;
import com.loopers.infrastructure.coupon.CouponJpaRepository;
import com.loopers.infrastructure.coupon.IssuedCouponJpaRepository;
import com.loopers.infrastructure.user.UserEntity;
import com.loopers.infrastructure.user.UserJpaRepository;
import com.loopers.utils.DatabaseCleanUp;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * 전체 흐름 E2E 동시성 테스트.
 *
 * 실행 전 필요:
 *   1. docker-compose -f ./docker/infra-compose.yml up -d
 *   2. ./gradlew :apps:commerce-streamer:bootRun
 */
@Disabled("전체 E2E: docker infra + commerce-streamer 실행 후 @Disabled 제거")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CouponIssueConcurrencyE2ETest {

    private static final String COUPON_STOCK_KEY_PREFIX = "coupon:stock:";
    private static final int STOCK = 50;
    private static final int TOTAL_USERS = 100;
    private static final String RAW_PASSWORD = "Test1234!";
    private static final String HASHED_PASSWORD = new BCryptPasswordEncoder().encode(RAW_PASSWORD);

    @Autowired private TestRestTemplate testRestTemplate;
    @Autowired private CouponJpaRepository couponJpaRepository;
    @Autowired private UserJpaRepository userJpaRepository;
    @Autowired private IssuedCouponJpaRepository issuedCouponJpaRepository;
    @Autowired @Qualifier("redisTemplateMaster") private RedisTemplate<String, String> redisTemplate;
    @Autowired private DatabaseCleanUp databaseCleanUp;
    @Autowired private RedisCleanUp redisCleanUp;

    private CouponEntity coupon;
    private List<String> userLoginIds;

    @BeforeEach
    void setUp() {
        coupon = couponJpaRepository.save(new CouponEntity(
            "선착순 쿠폰", CouponType.FIXED, BigDecimal.valueOf(1000),
            BigDecimal.valueOf(5000), ZonedDateTime.now().plusDays(30), 0
        ));

        List<UserEntity> users = new ArrayList<>();
        for (int i = 0; i < TOTAL_USERS; i++) {
            users.add(new UserEntity(
                "concuruser" + i, HASHED_PASSWORD,
                "유저" + i, LocalDate.of(1990, 1, 1), "concuruser" + i + "@test.com"
            ));
        }
        userJpaRepository.saveAll(users);
        userLoginIds = users.stream().map(UserEntity::getLoginId).toList();

        redisTemplate.opsForValue().set(COUPON_STOCK_KEY_PREFIX + coupon.getId(), String.valueOf(STOCK));
    }

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        redisCleanUp.truncateAll();
    }

    @DisplayName("선착순 50개 쿠폰에 100명이 API를 동시 호출하면, 모두 202를 받고 실제 발급은 50건만 된다.")
    @Test
    void limitsCouponIssuance_whenConcurrentApiRequests() throws InterruptedException {
        // Arrange: 100명이 동시에 발급 요청 준비
        CountDownLatch ready = new CountDownLatch(TOTAL_USERS);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(TOTAL_USERS);
        List<Integer> statusCodes = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < TOTAL_USERS; i++) {
            final String loginId = userLoginIds.get(i);
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    HttpHeaders headers = new HttpHeaders();
                    headers.set("X-Loopers-LoginId", loginId);
                    headers.set("X-Loopers-LoginPw", RAW_PASSWORD);
                    ResponseEntity<Void> response = testRestTemplate.exchange(
                        "/api/v1/coupons/" + coupon.getId() + "/issue",
                        HttpMethod.POST,
                        new HttpEntity<>(null, headers),
                        Void.class
                    );
                    statusCodes.add(response.getStatusCode().value());
                } catch (Exception e) {
                    statusCodes.add(500);
                }
            });
        }

        // Act: 동시 발사 → commerce-streamer가 Kafka 메시지 소비하여 50건 발급 완료까지 대기
        ready.await();
        start.countDown();
        executor.shutdown();
        executor.awaitTermination(30, SECONDS);

        await().atMost(60, SECONDS).until(() -> issuedCouponJpaRepository.count() >= STOCK);
        Thread.sleep(3_000);

        // Assert
        long issuedCount = issuedCouponJpaRepository.count();
        String remainingStock = redisTemplate.opsForValue().get(COUPON_STOCK_KEY_PREFIX + coupon.getId());

        assertAll(
            () -> assertThat(statusCodes).hasSize(TOTAL_USERS).allMatch(code -> code == 202),
            () -> assertThat(issuedCount).isEqualTo(STOCK),
            () -> assertThat(remainingStock).isEqualTo("0")
        );
    }
}
