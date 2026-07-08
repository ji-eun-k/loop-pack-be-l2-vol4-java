package com.loopers.infrastructure.queue;

import com.loopers.application.queue.QueueProperties;
import com.loopers.application.queue.QueueScheduler;
import com.loopers.application.queue.QueueService;
import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.domain.queue.JitterDelay;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.RedisCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(RedisTestContainersConfig.class)
class QueueSchedulerIntegrationTest {

    @MockitoBean private JitterDelay jitterDelay; // sleep 없이 테스트

    @Autowired private QueueScheduler queueScheduler;
    @Autowired private QueueService queueService;
    @Autowired private EntryTokenRepository entryTokenRepository;
    @Autowired private QueueProperties queueProperties;
    @Autowired private RedisCleanUp redisCleanUp;

    @AfterEach
    void tearDown() {
        redisCleanUp.truncateAll();
    }

    @DisplayName("issueEntryTokens()를 실행할 때,")
    @Nested
    class IssueEntryTokens {

        @DisplayName("대기열에 유저가 있으면 입장 토큰을 발급한다.")
        @Test
        void issuesToken_whenUserIsInQueue() {
            // arrange
            queueService.enter(1L);

            // act
            queueScheduler.issueEntryTokens();

            // assert
            assertThat(entryTokenRepository.find(1L)).isPresent();
        }

        @DisplayName("대기열이 비어 있으면 토큰을 발급하지 않는다.")
        @Test
        void doesNotIssueToken_whenQueueIsEmpty() {
            // act
            queueScheduler.issueEntryTokens();

            // assert
            assertThat(entryTokenRepository.find(1L)).isEmpty();
        }

        @DisplayName("여러 유저가 대기 중일 때 모두 토큰을 발급받는다.")
        @Test
        void issuesTokensForAllUsers_whenMultipleUsersInQueue() throws InterruptedException {
            // arrange
            for (long i = 1; i <= 3; i++) {
                queueService.enter(i);
                Thread.sleep(2);
            }

            // act
            queueScheduler.issueEntryTokens();

            // assert
            for (long i = 1; i <= 3; i++) {
                assertThat(entryTokenRepository.find(i)).isPresent();
            }
        }

        @DisplayName("배치 크기보다 많은 유저가 대기 중일 때, 배치 크기만큼만 토큰을 발급하고 나머지는 대기한다.")
        @Test
        void issuesOnlyBatchSizeTokens_whenQueueExceedsBatchSize() {
            // arrange - batchSize + 1명 진입
            int batchSize = queueProperties.getBatchSize();
            int totalUsers = batchSize + 1;
            for (long i = 1; i <= totalUsers; i++) {
                queueService.enter(i);
            }

            // act
            queueScheduler.issueEntryTokens();

            // assert - 정확히 batchSize만큼만 토큰 발급, 나머지 1명은 여전히 대기
            long issuedCount = 0;
            for (long i = 1; i <= totalUsers; i++) {
                if (entryTokenRepository.find(i).isPresent()) {
                    issuedCount++;
                }
            }
            assertThat(issuedCount).isEqualTo(batchSize);
        }
    }
}
