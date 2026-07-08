package com.loopers.application.queue;

import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.domain.queue.JitterDelay;
import com.loopers.domain.queue.QueueRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
@RequiredArgsConstructor
public class QueueScheduler {

    private final QueueRepository queueRepository;
    private final EntryTokenRepository entryTokenRepository;
    private final QueueProperties queueProperties;
    private final JitterDelay jitterDelay;

    @Scheduled(fixedDelayString = "${queue.scheduler-interval-ms}")
    public void issueEntryTokens() {
        List<Long> userIds = queueRepository.popOldest(queueProperties.getBatchSize());
        for (Long userId : userIds) {
            // 동시에 몰리는 주문 API 호출을 분산시키기 위한 jitter
            long jitterMillis = ThreadLocalRandom.current().nextLong(0, 301);
            jitterDelay.delay(jitterMillis);
            entryTokenRepository.save(userId, UUID.randomUUID().toString(), queueProperties.getTokenTtlSeconds());
        }
    }
}
