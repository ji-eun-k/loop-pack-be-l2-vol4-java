package com.loopers.domain.queue;

public record QueuePositionResult(
        QueueStatus status,
        long position,
        long waitingCount,
        long nextPollAfterMs,
        long estimatedWaitSeconds,
        String entryToken
) {}
