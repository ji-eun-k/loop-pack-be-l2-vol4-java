package com.loopers.interfaces.api;

import com.loopers.application.user.UserService;
import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.interfaces.api.queue.QueueV1Dto;
import com.loopers.interfaces.api.user.UserV1Dto;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(RedisTestContainersConfig.class)
public class QueueV1ApiE2ETest {

    private static final String QUEUE_V1_PATH = "/api/v1/queue";
    private static final String USER_V1_PATH = "/api/v1/users";
    private static final String LOGIN_ID_1 = "quser1";
    private static final String LOGIN_ID_2 = "quser2";
    private static final String LOGIN_PW = "pAssWord1!";

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @Autowired
    private RedisCleanUp redisCleanUp;

    @Autowired
    private UserService userService;

    @Autowired
    private EntryTokenRepository entryTokenRepository;

    @BeforeEach
    void setUp() {
        join(LOGIN_ID_1, "q1@test.com");
        join(LOGIN_ID_2, "q2@test.com");
    }

    @AfterEach
    void tearDown() {
        redisCleanUp.truncateAll();
        databaseCleanUp.truncateAllTables();
    }

    private void join(String loginId, String email) {
        testRestTemplate.exchange(
                USER_V1_PATH, HttpMethod.POST,
                new HttpEntity<>(new UserV1Dto.UserJoinRequest(loginId, LOGIN_PW, "Queuer", LocalDate.of(2000, 1, 1), email)),
                new ParameterizedTypeReference<ApiResponse<UserV1Dto.UserResponse>>() {}
        );
    }

    private HttpHeaders authHeaders(String loginId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Loopers-LoginId", loginId);
        headers.set("X-Loopers-LoginPw", LOGIN_PW);
        return headers;
    }

    @DisplayName("POST /api/v1/queue/enter")
    @Nested
    class PostEnterQueue {

        @DisplayName("처음 진입하면 WAITING 상태와 position 1을 반환한다.")
        @Test
        void returnWaiting_whenFirstEnter() {
            // act
            ResponseEntity<ApiResponse<QueueV1Dto.EnterResponse>> response = testRestTemplate.exchange(
                    QUEUE_V1_PATH + "/enter", HttpMethod.POST,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<>() {}
            );

            // assert
            assertAll(
                    () -> assertTrue(response.getStatusCode().is2xxSuccessful()),
                    () -> assertThat(response.getBody().data().status()).isEqualTo("WAITING"),
                    () -> assertThat(response.getBody().data().position()).isEqualTo(1L),
                    () -> assertThat(response.getBody().data().waitingCount()).isEqualTo(1L)
            );
        }

        @DisplayName("재진입하면 position이 맨 뒤로 밀린다.")
        @Test
        void moveToBack_whenReEntering() throws InterruptedException {
            // arrange
            testRestTemplate.exchange(QUEUE_V1_PATH + "/enter", HttpMethod.POST,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<ApiResponse<QueueV1Dto.EnterResponse>>() {});
            Thread.sleep(2);
            testRestTemplate.exchange(QUEUE_V1_PATH + "/enter", HttpMethod.POST,
                    new HttpEntity<>(authHeaders(LOGIN_ID_2)),
                    new ParameterizedTypeReference<ApiResponse<QueueV1Dto.EnterResponse>>() {});
            Thread.sleep(2);

            // act - quser1 재진입
            ResponseEntity<ApiResponse<QueueV1Dto.EnterResponse>> response = testRestTemplate.exchange(
                    QUEUE_V1_PATH + "/enter", HttpMethod.POST,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<>() {}
            );

            // assert
            assertAll(
                    () -> assertTrue(response.getStatusCode().is2xxSuccessful()),
                    () -> assertThat(response.getBody().data().status()).isEqualTo("WAITING"),
                    () -> assertThat(response.getBody().data().position()).isEqualTo(2L)
            );
        }
    }

    @DisplayName("GET /api/v1/queue/position")
    @Nested
    class GetQueuePosition {

        @DisplayName("대기 중인 유저의 순번과 전체 대기 인원을 반환한다.")
        @Test
        void returnWaitingInfo_whenUserIsWaiting() {
            // arrange
            testRestTemplate.exchange(QUEUE_V1_PATH + "/enter", HttpMethod.POST,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<ApiResponse<QueueV1Dto.EnterResponse>>() {});

            // act
            ResponseEntity<ApiResponse<QueueV1Dto.PositionResponse>> response = testRestTemplate.exchange(
                    QUEUE_V1_PATH + "/position", HttpMethod.GET,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<>() {}
            );

            // assert
            assertAll(
                    () -> assertTrue(response.getStatusCode().is2xxSuccessful()),
                    () -> assertThat(response.getBody().data().status()).isEqualTo("WAITING"),
                    () -> assertThat(response.getBody().data().position()).isEqualTo(1L),
                    () -> assertThat(response.getBody().data().waitingCount()).isEqualTo(1L)
            );
        }

        @DisplayName("대기열에 없는 유저는 NOT_IN_QUEUE 상태를 반환한다.")
        @Test
        void returnNotInQueue_whenUserNotInQueue() {
            // act
            ResponseEntity<ApiResponse<QueueV1Dto.PositionResponse>> response = testRestTemplate.exchange(
                    QUEUE_V1_PATH + "/position", HttpMethod.GET,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<>() {}
            );

            // assert
            assertAll(
                    () -> assertTrue(response.getStatusCode().is2xxSuccessful()),
                    () -> assertThat(response.getBody().data().status()).isEqualTo("NOT_IN_QUEUE")
            );
        }

        @DisplayName("입장 토큰이 발급된 유저는 ACTIVE 상태와 entryToken을 반환한다.")
        @Test
        void returnActiveWithToken_whenEntryTokenIssued() {
            // arrange
            Long userId = userService.getUser(LOGIN_ID_1, LOGIN_PW).getId();
            entryTokenRepository.save(userId, "test-entry-token", 300);

            // act
            ResponseEntity<ApiResponse<QueueV1Dto.PositionResponse>> response = testRestTemplate.exchange(
                    QUEUE_V1_PATH + "/position", HttpMethod.GET,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<>() {}
            );

            // assert
            assertAll(
                    () -> assertTrue(response.getStatusCode().is2xxSuccessful()),
                    () -> assertThat(response.getBody().data().status()).isEqualTo("ACTIVE"),
                    () -> assertThat(response.getBody().data().entryToken()).isEqualTo("test-entry-token")
            );
        }

        @DisplayName("대기 중인 유저의 응답에 예상 대기 시간이 포함된다.")
        @Test
        void returnsEstimatedWaitSeconds_whenUserIsWaiting() {
            // arrange
            testRestTemplate.exchange(QUEUE_V1_PATH + "/enter", HttpMethod.POST,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<ApiResponse<QueueV1Dto.EnterResponse>>() {});

            // act
            ResponseEntity<ApiResponse<QueueV1Dto.PositionResponse>> response = testRestTemplate.exchange(
                    QUEUE_V1_PATH + "/position", HttpMethod.GET,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<>() {}
            );

            // assert - position=1, batchSize=75, interval=1s → ceil(1/75*1) = 1초
            assertAll(
                    () -> assertTrue(response.getStatusCode().is2xxSuccessful()),
                    () -> assertThat(response.getBody().data().estimatedWaitSeconds()).isGreaterThan(0L)
            );
        }

        @DisplayName("대기 중인 유저의 응답에 다음 폴링 간격이 포함된다.")
        @Test
        void returnsNextPollAfterMs_whenUserIsWaiting() {
            // arrange
            testRestTemplate.exchange(QUEUE_V1_PATH + "/enter", HttpMethod.POST,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<ApiResponse<QueueV1Dto.EnterResponse>>() {});

            // act
            ResponseEntity<ApiResponse<QueueV1Dto.PositionResponse>> response = testRestTemplate.exchange(
                    QUEUE_V1_PATH + "/position", HttpMethod.GET,
                    new HttpEntity<>(authHeaders(LOGIN_ID_1)),
                    new ParameterizedTypeReference<>() {}
            );

            // assert - position=1 (50 이하) → 1_000ms
            assertAll(
                    () -> assertTrue(response.getStatusCode().is2xxSuccessful()),
                    () -> assertThat(response.getBody().data().nextPollAfterMs()).isEqualTo(1_000L)
            );
        }
    }
}
