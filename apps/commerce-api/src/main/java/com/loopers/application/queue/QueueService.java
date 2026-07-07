package com.loopers.application.queue;

import com.loopers.domain.queue.EntryTokenRepository;
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

    private final QueueRepository queueRepository;
    private final EntryTokenRepository entryTokenRepository;
    private final QueueProperties queueProperties;

    public QueueEntryResult enter(Long userId) {
        List<Long> result = queueRepository.enter(userId);
        long rank = result.get(0);    // 0-based
        long total = result.get(1);
        long position = rank + 1;     // 1-based
        return new QueueEntryResult(QueueStatus.WAITING, position, total, calculateEstimatedWait(position));
    }

    public QueuePositionResult getPosition(Long userId) {
        return entryTokenRepository.find(userId)
                .map(token -> new QueuePositionResult(QueueStatus.ACTIVE, 0, 0, 0, 0, token))
                .orElseGet(() -> queueRepository.findPositionSnapshot(userId)
                        .map(snapshot -> {
                            long position = snapshot.rank() + 1;
                            long total = snapshot.totalWaiting();
                            return new QueuePositionResult(
                                    QueueStatus.WAITING, position, total,
                                    calculateNextPollInterval(position),
                                    calculateEstimatedWait(position),
                                    null
                            );
                        })
                        .orElse(new QueuePositionResult(QueueStatus.NOT_IN_QUEUE, 0, 0, 0, 0, null))
                );
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
