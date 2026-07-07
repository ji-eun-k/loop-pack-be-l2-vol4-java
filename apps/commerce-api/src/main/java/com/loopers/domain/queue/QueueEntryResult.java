package com.loopers.domain.queue;

public record QueueEntryResult(
        QueueStatus status,
        long position,
        long waitingCount,
        long estimatedWaitSeconds
) {}
