package com.loopers.application.queue;

import com.loopers.application.queue.QueueEntry;
import com.loopers.application.queue.QueuePosition;
import com.loopers.application.queue.QueueProperties;
import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.domain.queue.QueueStatus;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@SpringBootTest
@Import(RedisTestContainersConfig.class)
class QueueServiceIntegrationTest {

    @Autowired
    private QueueService queueService;

    @Autowired
    private EntryTokenRepository entryTokenRepository;

    @Autowired
    private QueueProperties queueProperties;

    @Autowired
    private RedisCleanUp redisCleanUp;

    @AfterEach
    void tearDown() {
        redisCleanUp.truncateAll();
    }

    @DisplayName("대기열 진입(enter)할 때,")
    @Nested
    class Enter {

        @DisplayName("처음 진입하면 WAITING 상태와 position 1을 반환한다.")
        @Test
        void returnWaiting_whenFirstEnter() {
            // act
            QueueEntry result = queueService.enter(1L);

            // assert
            assertAll(
                    () -> assertThat(result.status()).isEqualTo(QueueStatus.WAITING),
                    () -> assertThat(result.position()).isEqualTo(1L),
                    () -> assertThat(result.waitingCount()).isEqualTo(1L)
            );
        }

        @DisplayName("여러 유저가 순서대로 진입하면 FIFO 순번이 부여된다.")
        @Test
        void assignFifoRank_whenMultipleUsersEnter() {
            // act
            QueueEntry first = queueService.enter(1L);
            QueueEntry second = queueService.enter(2L);
            QueueEntry third = queueService.enter(3L);

            // assert
            assertAll(
                    () -> assertThat(first.position()).isEqualTo(1L),
                    () -> assertThat(second.position()).isEqualTo(2L),
                    () -> assertThat(third.position()).isEqualTo(3L),
                    () -> assertThat(third.waitingCount()).isEqualTo(3L)
            );
        }

        @DisplayName("이미 대기 중인 유저가 재진입하면 position이 맨 뒤로 밀린다.")
        @Test
        void moveToBack_whenReEntering() {
            // arrange
            queueService.enter(1L);
            queueService.enter(2L);

            // act - userId=1 재진입 → score 갱신되어 userId=2 뒤로 밀림
            QueueEntry reEntry = queueService.enter(1L);

            // assert
            assertThat(reEntry.position()).isEqualTo(2L);
        }
    }

    @DisplayName("순번 조회(getPosition)할 때,")
    @Nested
    class GetPosition {

        @DisplayName("대기 중인 유저의 순번과 전체 대기 인원을 반환한다.")
        @Test
        void returnWaitingInfo_whenUserIsWaiting() {
            // arrange
            queueService.enter(1L);
            queueService.enter(2L);

            // act
            QueuePosition result = queueService.getPosition(1L);

            // assert
            assertAll(
                    () -> assertThat(result.status()).isEqualTo(QueueStatus.WAITING),
                    () -> assertThat(result.position()).isEqualTo(1L),
                    () -> assertThat(result.waitingCount()).isEqualTo(2L)
            );
        }

        @DisplayName("대기열에 없는 유저는 NOT_IN_QUEUE 상태를 반환한다.")
        @Test
        void returnNotInQueue_whenUserNotInQueue() {
            // act
            QueuePosition result = queueService.getPosition(999L);

            // assert
            assertThat(result.status()).isEqualTo(QueueStatus.NOT_IN_QUEUE);
        }

        @DisplayName("입장 토큰이 발급된 유저는 ACTIVE 상태와 토큰을 반환한다.")
        @Test
        void returnActiveWithToken_whenEntryTokenExists() {
            // arrange
            entryTokenRepository.save(1L, "test-token", 300);

            // act
            QueuePosition result = queueService.getPosition(1L);

            // assert
            assertAll(
                    () -> assertThat(result.status()).isEqualTo(QueueStatus.ACTIVE),
                    () -> assertThat(result.entryToken()).isEqualTo("test-token")
            );
        }

        @DisplayName("대기 중인 유저의 예상 대기 시간은 position / batchSize 로 계산된다.")
        @Test
        void returnsEstimatedWaitSeconds_basedOnPosition() {
            // arrange - position=1
            queueService.enter(1L);

            // act
            QueuePosition result = queueService.getPosition(1L);

            // assert - ceil(1 / batchSize * intervalSeconds)
            long expectedWait = (long) Math.ceil(1.0 / queueProperties.getBatchSize() * queueProperties.getSchedulerIntervalMs() / 1000.0);
            assertThat(result.estimatedWaitSeconds()).isEqualTo(expectedWait);
        }

        @DisplayName("대기 중인 유저의 다음 폴링 간격은 순번에 따라 달라진다.")
        @Test
        void returnsNextPollAfterMs_basedOnPosition() {
            // arrange - position=1 (50 이하)
            queueService.enter(1L);

            // act
            QueuePosition result = queueService.getPosition(1L);

            // assert
            assertThat(result.nextPollAfterMs()).isEqualTo(1_000L);
        }

        @DisplayName("TTL이 만료된 입장 토큰은 유효하지 않아 NOT_IN_QUEUE 상태를 반환한다.")
        @Test
        void returnNotInQueue_whenEntryTokenExpired() throws InterruptedException {
            // arrange - TTL 1초로 저장
            entryTokenRepository.save(1L, "expiring-token", 1);
            Thread.sleep(1100); // TTL 만료 대기

            // act
            QueuePosition result = queueService.getPosition(1L);

            // assert
            assertThat(result.status()).isEqualTo(QueueStatus.NOT_IN_QUEUE);
        }
    }

    @DisplayName("동시에 여러 유저가 대기열에 진입할 때,")
    @Nested
    class ConcurrentEnter {

        @DisplayName("모든 순번이 중복 없이 부여된다.")
        @Test
        void assignUniquePositions_whenConcurrentEnter() throws InterruptedException {
            // arrange
            int threadCount = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            List<Long> positions = Collections.synchronizedList(new ArrayList<>());
            List<String> errors = Collections.synchronizedList(new ArrayList<>());

            for (long i = 1; i <= threadCount; i++) {
                final long userId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        QueueEntry entry = queueService.enter(userId);
                        positions.add(entry.position());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        errors.add("userId=" + userId + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // act - 모든 스레드 동시 시작
            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);
            executor.shutdown();

            // assert - 순번 중복 없음, 전원 정상 진입
            assertAll(
                    () -> assertThat(errors).as("스레드 예외 발생: " + errors).isEmpty(),
                    () -> assertThat(positions).hasSize(threadCount),
                    () -> assertThat(new HashSet<>(positions)).hasSize(threadCount)
            );
        }
    }
}
