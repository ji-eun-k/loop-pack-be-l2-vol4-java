package com.loopers.application.queue;

import com.loopers.domain.queue.QueueEntryResult;
import com.loopers.domain.queue.QueuePositionResult;
import com.loopers.domain.queue.QueueRepository;
import com.loopers.domain.queue.QueueStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class QueueService {

    private static final int STATUS_ACTIVE = 0;
    private static final int STATUS_WAITING = 1;
    private static final int STATUS_NOT_IN_QUEUE = 2;

    private final QueueRepository queueRepository;
    private final QueueProperties queueProperties;

    public QueueEntryResult enter(Long userId) {
        List<Long> result = queueRepository.enter(userId);
        long rank = result.get(0);    // 0-based
        long total = result.get(1);
        long position = rank + 1;     // 1-based
        return new QueueEntryResult(QueueStatus.WAITING, position, total, calculateEstimatedWait(position));
    }

    public QueuePositionResult getPosition(Long userId) {
        List<Long> result = queueRepository.getPosition(userId);
        int statusCode = result.get(0).intValue();

        return switch (statusCode) {
            case STATUS_ACTIVE -> new QueuePositionResult(QueueStatus.ACTIVE, 0, 0, 0, 0);
            case STATUS_WAITING -> {
                long position = result.get(1) + 1;  // 0-based → 1-based
                long total = result.get(2);
                yield new QueuePositionResult(
                        QueueStatus.WAITING, position, total,
                        calculateNextPollInterval(position),
                        calculateEstimatedWait(position)
                );
            }
            case STATUS_NOT_IN_QUEUE -> new QueuePositionResult(QueueStatus.NOT_IN_QUEUE, 0, 0, 0, 0);
            default -> throw new IllegalStateException("알 수 없는 대기열 상태 코드: " + statusCode);
        };
    }

    private long calculateEstimatedWait(long position) {
        return (long) Math.ceil((double) position / queueProperties.getBatchSize());
    }

    private long calculateNextPollInterval(long position) {
        if (position <= 50) return 1_000;
        if (position <= 500) return 3_000;
        if (position <= 5_000) return 5_000;
        return 10_000;
    }
}
