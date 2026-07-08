package com.loopers.application.queue;

import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.domain.queue.QueueRepository;
import com.loopers.domain.queue.QueueStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class QueueService {

    private final QueueRepository queueRepository;
    private final EntryTokenRepository entryTokenRepository;
    private final QueueProperties queueProperties;

    public QueueEntry enter(Long userId) {
        List<Long> result = queueRepository.enter(userId);
        long rank = result.get(0);    // 0-based
        long total = result.get(1);
        long position = rank + 1;     // 1-based
        return new QueueEntry(QueueStatus.WAITING, position, total, calculateEstimatedWait(position));
    }

    public QueuePosition getPosition(Long userId) {
        // ① 입장 토큰이 있으면 → ACTIVE (스케줄러가 이미 입장 허가한 상태)
        Optional<String> token = entryTokenRepository.find(userId);
        if (token.isPresent()) {
            return new QueuePosition(QueueStatus.ACTIVE, 0, 0, 0, 0, token.get());
        }

        // ② 대기열 순번 조회 → WAITING
        return queueRepository.findPositionSnapshot(userId)
                .map(snapshot -> {
                    long position = snapshot.rank() + 1; // 0-based → 1-based
                    long total = snapshot.totalWaiting();
                    return new QueuePosition(
                            QueueStatus.WAITING, position, total,
                            calculateNextPollInterval(position),
                            calculateEstimatedWait(position),
                            null
                    );
                })
                // ③ 대기열에도 없으면 → NOT_IN_QUEUE
                .orElse(new QueuePosition(QueueStatus.NOT_IN_QUEUE, 0, 0, 0, 0, null));
    }

    private long calculateEstimatedWait(long position) {
        // 예상 대기 시간(초) = position / batchSize * schedulerInterval(초)
        // 예) position=150, batchSize=75, interval=1s → 150/75*1 = 2초
        double intervalSeconds = queueProperties.getSchedulerIntervalMs() / 1000.0;
        return (long) Math.ceil((double) position / queueProperties.getBatchSize() * intervalSeconds);
    }

    private long calculateNextPollInterval(long position) {
        if (position <= 50) return 1_000;
        if (position <= 500) return 3_000;
        if (position <= 5_000) return 5_000;
        return 10_000;
    }
}
