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
    private final SseEmitterRegistry sseEmitterRegistry;
    private final QueueService queueService;

    @Scheduled(fixedDelayString = "${queue.scheduler-interval-ms}")
    public void issueEntryTokens() {
        List<Long> userIds = queueRepository.popOldest(queueProperties.getBatchSize());
        for (Long userId : userIds) {
            // 동시에 몰리는 주문 API 호출을 분산시키기 위한 jitter
            long jitterMillis = ThreadLocalRandom.current().nextLong(0, 301);
            jitterDelay.delay(jitterMillis);
            String token = UUID.randomUUID().toString();
            entryTokenRepository.save(userId, token, queueProperties.getTokenTtlSeconds());
            // ACTIVE 이벤트 전송 후 해당 emitter는 registry에서 제거된다
            sseEmitterRegistry.sendActive(userId, token);
        }

        // 토큰 발급 후 아직 대기 중인 SSE 연결자들에게 갱신된 순번을 PUSH한다.
        // sendActive()로 이미 제거된 userId는 registry에 없으므로 자동으로 제외된다.
        for (Long waitingUserId : sseEmitterRegistry.getRegisteredUserIds()) {
            QueuePosition position = queueService.getPosition(waitingUserId);
            sseEmitterRegistry.sendPosition(waitingUserId, position);
        }
    }
}
