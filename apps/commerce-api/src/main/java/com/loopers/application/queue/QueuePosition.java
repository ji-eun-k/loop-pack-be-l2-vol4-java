package com.loopers.application.queue;

import com.loopers.domain.queue.QueueStatus;

public record QueuePosition(
        QueueStatus status,
        long position,
        long waitingCount,
        long nextPollAfterMs,
        long estimatedWaitSeconds,
        String entryToken
) {}
