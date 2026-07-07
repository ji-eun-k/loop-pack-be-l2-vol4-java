package com.loopers.application.queue;

import com.loopers.domain.queue.QueueEntryResult;
import com.loopers.domain.queue.QueuePositionResult;
import com.loopers.domain.queue.QueueStatus;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@SpringBootTest
@Import(RedisTestContainersConfig.class)
class QueueServiceIntegrationTest {

    @Autowired
    private QueueService queueService;

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
            QueueEntryResult result = queueService.enter(1L);

            // assert
            assertAll(
                    () -> assertThat(result.status()).isEqualTo(QueueStatus.WAITING),
                    () -> assertThat(result.position()).isEqualTo(1L),
                    () -> assertThat(result.waitingCount()).isEqualTo(1L)
            );
        }

        @DisplayName("여러 유저가 순서대로 진입하면 FIFO 순번이 부여된다.")
        @Test
        void assignFifoRank_whenMultipleUsersEnter() throws InterruptedException {
            // act
            QueueEntryResult first = queueService.enter(1L);
            Thread.sleep(2);
            QueueEntryResult second = queueService.enter(2L);
            Thread.sleep(2);
            QueueEntryResult third = queueService.enter(3L);

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
        void moveToBack_whenReEntering() throws InterruptedException {
            // arrange
            queueService.enter(1L);
            Thread.sleep(2);
            queueService.enter(2L);
            Thread.sleep(2);

            // act - userId=1 재진입 → score 갱신되어 userId=2 뒤로 밀림
            QueueEntryResult reEntry = queueService.enter(1L);

            // assert
            assertThat(reEntry.position()).isEqualTo(2L);
        }
    }

    @DisplayName("순번 조회(getPosition)할 때,")
    @Nested
    class GetPosition {

        @DisplayName("대기 중인 유저의 순번과 전체 대기 인원을 반환한다.")
        @Test
        void returnWaitingInfo_whenUserIsWaiting() throws InterruptedException {
            // arrange
            queueService.enter(1L);
            Thread.sleep(2);
            queueService.enter(2L);

            // act
            QueuePositionResult result = queueService.getPosition(1L);

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
            QueuePositionResult result = queueService.getPosition(999L);

            // assert
            assertThat(result.status()).isEqualTo(QueueStatus.NOT_IN_QUEUE);
        }
    }
}
